import requests

from pathlib import Path
from enum import Enum
from typing import Callable, Mapping, MutableMapping
from threading import Lock

from .common import chunk_name, CAN_RETRY_STATUS_CODES, CanRetryError
from .retry import Retry
from .segment import Segment
from .range_downloader import RangeDownloader, RangeNotSupportedError
from .utils import clean_path


class _SingletonPhase(Enum):
  NOT_STARTED = 0
  DOWNLOADING = 1
  COMPLETED = 2
  FAILED = 3

class File:
  def __init__(
        self,
        url: str,
        file_path: Path,
        min_segment_length: int,
        retry: Retry,
        timeout: float,
        once_fetch_size: int,
        excepted_etag: str | None = None,
        headers: Mapping[str, str | bytes | None] | None = None,
        cookies: MutableMapping[str, str] | None = None,
      ) -> None:

    assert not file_path.exists(), "file already exists"

    self._url: str = url
    self._file_path: Path = file_path
    self._min_segment_length: int = min_segment_length
    self._retry: Retry = retry
    self._timeout: float = timeout
    self._once_fetch_size: int = once_fetch_size
    self._headers: Mapping[str, str | bytes | None] | None = headers
    self._cookies: MutableMapping[str, str] | None = cookies

    self._did_stop: bool = False
    self._singleton_lock: Lock = Lock()
    self._singleton_phase: _SingletonPhase = _SingletonPhase.NOT_STARTED
    self._range_lock: Lock = Lock()
    self._range_downloader: RangeDownloader | None = None
    try:
      self._range_downloader = RangeDownloader(
        url=url,
        file_path=file_path,
        min_segment_length=min_segment_length,
        retry=retry,
        timeout=timeout,
        once_fetch_size=once_fetch_size,
        excepted_etag=excepted_etag,
        headers=headers,
        cookies=cookies,
      )
    except RangeNotSupportedError:
      pass

  def pop_downloading_task(self) -> Callable[[], None] | None:
    if self._did_stop:
      return None

    with self._range_lock:
      range_downloader = self._range_downloader
      if range_downloader is not None:
        segment = range_downloader.serial.pop_segment()
        if not segment:
          return None
        return lambda: self._download_segment(
          range_downloader=range_downloader,
          segment=segment,
        )

    download_file = self._file_path.parent / chunk_name(self._file_path, 0)
    with self._singleton_lock:
      phase = self._singleton_phase
      if phase == _SingletonPhase.DOWNLOADING or \
         phase == _SingletonPhase.COMPLETED:
        return None
      if phase == _SingletonPhase.FAILED:
        download_file.unlink(missing_ok=True)

    return lambda: self._download_file(download_file)

  def _download_segment(self, range_downloader: RangeDownloader, segment: Segment) -> None:
    try:
      if self._did_stop:
        return
      try:
        range_downloader.download_segment(segment)

      except RangeNotSupportedError:
        with self._range_lock:
          self._range_downloader = None
          range_downloader.serial.dispose()
    finally:
      segment.dispose()

  def _download_file(self, file_path: Path) -> None:
    try:
      with self._singleton_lock:
        self._singleton_phase = _SingletonPhase.DOWNLOADING

      resp = requests.Session().get(
        stream=True,
        url=self._url,
        headers=self._headers,
        cookies=self._cookies,
        timeout=self._timeout,
      )
      if resp.status_code in CAN_RETRY_STATUS_CODES:
        raise CanRetryError(f"HTTP {resp.status_code} - {resp.reason}")
      resp.raise_for_status()

      with open(file_path, "wb") as file:
        for chunk in resp.iter_content(self._once_fetch_size):
          if len(chunk) == 0:
            break
          if self._did_stop:
            with self._singleton_lock:
              self._singleton_phase = _SingletonPhase.FAILED
            return
          file.write(chunk)

      with self._singleton_lock:
        self._singleton_phase = _SingletonPhase.COMPLETED

    except Exception as error:
      with self._singleton_lock:
        self._singleton_phase = _SingletonPhase.FAILED
      raise error

  def try_complete(self) -> Path | None:
    chunk_paths: list[Path] = []

    if self._range_downloader is not None:
      serial = self._range_downloader.serial
      if not serial.is_completed:
        return None
      for description in serial.snapshot():
        chunk_path = self._file_path.parent / chunk_name(self._file_path, description.offset)
        chunk_paths.append(chunk_path)
    else:
      with self._singleton_lock:
        if self._singleton_phase != _SingletonPhase.COMPLETED:
          return None
      file_path = self._file_path.parent / chunk_name(self._file_path, 0)
      chunk_paths.append(file_path)

    clean_path(self._file_path)
    try:
      with open(self._file_path, "wb") as output:
        for chunk_path in chunk_paths:
          with open(chunk_path, "rb") as input:
            while True:
              chunk = input.read(self._once_fetch_size)
              if not chunk:
                break
              output.write(chunk)

    except Exception as err:
      self._file_path.unlink(missing_ok=True)
      raise err

    for chunk_path in chunk_paths:
      chunk_path.unlink(missing_ok=True)
    return self._file_path

  def stop(self) -> None:
    if self._did_stop:
      return
    self._did_stop = True

    if self._range_downloader:
      self._range_downloader.serial.dispose()