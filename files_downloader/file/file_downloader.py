import requests
import shutil

from pathlib import Path
from enum import Enum
from typing import Callable
from threading import Lock

from ..common import is_exception_can_retry, CAN_RETRY_STATUS_CODES, HTTPOptions
from .common import chunk_name
from .errors import CanRetryError, RangeNotSupportedError
from .segment import Segment
from .range_downloader import RangeDownloader
from .utils import clean_path


class _SingletonPhase(Enum):
  NOT_STARTED = 0
  POPPED_TASK = 1
  COMPLETED = 2
  FAILED = 3

class FileDownloader:
  def __init__(
        self,
        file_path: Path,
        http_options: HTTPOptions,
        min_segment_length: int,
        once_fetch_size: int,
        excepted_etag: str | None = None,
      ) -> None:

    assert not file_path.exists(), "file already exists"

    self._file_path: Path = file_path
    self._http_options: HTTPOptions = http_options
    self._min_segment_length: int = min_segment_length
    self._once_fetch_size: int = once_fetch_size

    self._did_dispose: bool = False
    self._singleton_lock: Lock = Lock()
    self._singleton_phase: _SingletonPhase = _SingletonPhase.NOT_STARTED
    self._range_lock: Lock = Lock()
    self._did_cancel_range: bool = False
    self._range_downloader: RangeDownloader | None = None
    try:
      self._range_downloader = RangeDownloader(
        file_path=file_path,
        http_options=http_options,
        min_segment_length=min_segment_length,
        once_fetch_size=once_fetch_size,
        excepted_etag=excepted_etag,
      )
    except RangeNotSupportedError:
      pass

  def pop_downloading_task(self) -> Callable[[], None] | None:
    if self._did_dispose:
      return None

    with self._range_lock:
      range_downloader = self._validate_range_downloader()
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
      if phase == _SingletonPhase.POPPED_TASK or \
         phase == _SingletonPhase.COMPLETED:
        return None
      if phase == _SingletonPhase.FAILED:
        download_file.unlink(missing_ok=True)
      self._singleton_phase = _SingletonPhase.POPPED_TASK

    return lambda: self._download_file(download_file)

  def _download_segment(self, range_downloader: RangeDownloader, segment: Segment) -> None:
    try:
      if not self._did_dispose:
        range_downloader.download_segment(segment)

    except RangeNotSupportedError as error:
      if not error.is_canceled_by:
        with self._range_lock:
          range_downloader.serial.interrupt()
          self._did_cancel_range = True
      raise error

    finally:
      segment.dispose()

  def _download_file(self, file_path: Path) -> None:
    try:
      resp = requests.Session().get(
        stream=True,
        url=self._http_options.url,
        headers=self._http_options.headers,
        cookies=self._http_options.cookies,
        timeout=self._http_options.timeout,
      )
      if resp.status_code in CAN_RETRY_STATUS_CODES:
        raise CanRetryError(f"HTTP {resp.status_code} - {resp.reason}")
      resp.raise_for_status()

      with open(file_path, "wb") as file:
        try:
          for chunk in resp.iter_content(self._once_fetch_size):
            if len(chunk) == 0:
              break
            if self._did_dispose:
              with self._singleton_lock:
                self._singleton_phase = _SingletonPhase.FAILED
              return
            file.write(chunk)

        except Exception as error:
          if is_exception_can_retry(error):
            raise CanRetryError("Download file failed") from error
          else:
            raise error

      with self._singleton_lock:
        self._singleton_phase = _SingletonPhase.COMPLETED

    except Exception as error:
      with self._singleton_lock:
        self._singleton_phase = _SingletonPhase.FAILED
      raise error

  def try_complete(self) -> Path | None:
    chunk_paths: list[Path] = []
    with self._range_lock:
      range_downloader = self._validate_range_downloader()

    if range_downloader is not None:
      serial = range_downloader.serial
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

    if len(chunk_paths) == 1:
      source_path = chunk_paths[0]
      shutil.move(source_path, self._file_path)
    else:
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

  def dispose(self) -> None:
    if self._did_dispose:
      return
    self._did_dispose = True

    with self._range_lock:
      range_downloader = self._range_downloader
      if range_downloader is not None:
        range_downloader.serial.dispose()
        self._range_downloader = None
        self._did_cancel_range = False

  def _validate_range_downloader(self) -> RangeDownloader | None:
    range_downloader = self._range_downloader
    if self._did_cancel_range and range_downloader is not None:
      range_downloader.serial.dispose()
      range_downloader = None
      self._range_downloader = None

    return range_downloader