from pathlib import Path
from typing import Callable, Mapping, MutableMapping
from threading import Lock

from .common import chunk_name
from .retry import Retry
from .segment import Segment
from .range_downloader import RangeDownloader, RangeNotSupportedError
from .utils import clean_path

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

    assert "*" not in file_path.name, "file name cannot contain \"*\" character"
    assert not file_path.exists(), "file already exists"

    self._url: str = url
    self._file_path: Path = file_path
    self._min_segment_length: int = min_segment_length
    self._retry: Retry = retry
    self._timeout: float = timeout
    self._once_fetch_size: int = once_fetch_size
    self._headers: Mapping[str, str | bytes | None] | None = headers
    self._cookies: MutableMapping[str, str] | None = cookies

    self._stopping_lock: Lock = Lock()
    self._did_stop: bool = False
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

    return self._download_file

  def _download_segment(self, range_downloader: RangeDownloader, segment: Segment) -> None:
    try:
      with self._stopping_lock:
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

  def _download_file(self) -> None:
    raise NotImplementedError("TODO: 实现独自下载文件逻辑")

  def try_complete(self) -> Path | None:
    if self._range_downloader is None:
      raise NotImplementedError("TODO: 实现独自下载文件逻辑")

    serial = self._range_downloader.serial
    if not serial.is_completed:
      return None

    clean_path(self._file_path)
    chunk_paths: list[Path] = []
    for description in serial.snapshot():
      chunk_path = self._file_path.parent / chunk_name(self._file_path, description.offset)
      chunk_paths.append(chunk_path)

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
    with self._stopping_lock:
      if self._did_stop:
        return
      self._did_stop = True

    if not self._range_downloader:
      raise NotImplementedError("TODO: 实现独自下载文件逻辑")
    self._range_downloader.serial.dispose()