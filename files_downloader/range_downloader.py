import glob
import requests

from pathlib import Path
from typing import Callable, Mapping, MutableMapping
from threading import Lock, Event

from .segment import Serial, Segment, SegmentDescription
from .retry import Retry
from .common import chunk_name, DOWNLOADING_SUFFIX, CanRetryError
from .utils import clean_path


class RangeNotSupportedError(Exception):
  pass

# thread safe
class RangeDownloader:
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

    self._url: str = url
    self._file_path: Path = file_path
    self._min_segment_length: int = min_segment_length
    self._timeout: float = timeout
    self._once_fetch_size: int = once_fetch_size
    self._headers: Mapping[str, str | bytes | None] | None = headers
    self._cookies: MutableMapping[str, str] | None = cookies

    self._padding_lock: Lock = Lock()
    self._is_first_fetch: bool = True
    self._did_support_range: bool = False
    self._support_range_event: Event = Event()

    content_length, etag, range_useable = self._fetch_meta(retry)
    if content_length is None:
      raise ValueError("Content-Length header is missing in response")
    if not range_useable:
      raise RangeNotSupportedError()

    self._serial: Serial = self._create_serial(
      content_length=content_length,
      etag=etag,
      excepted_etag=excepted_etag,
    )

  def _fetch_meta(self, retry: Retry):
    resp = retry.request(
      request=lambda: requests.head(
        url=self._url,
        headers=self._headers,
        cookies=self._cookies,
        timeout=self._timeout,
      ),
    )
    content_length = resp.headers.get("Content-Length")
    etag = resp.headers.get("ETag")
    range_useable = resp.headers.get("Accept-Ranges") == "bytes"

    if content_length is not None:
      content_length = int(content_length)
    return content_length, etag, range_useable

  def _create_serial(self, content_length: int, etag: str | None, excepted_etag: str | None) -> Serial:
    descriptions: list[SegmentDescription] | None = None
    offsets: list[int] | None = None

    if etag is not None and excepted_etag is not None and excepted_etag != etag:
      for offset in self._search_offsets(content_length):
        clean_path(self._file_path.parent / chunk_name(self._file_path, offset))
    elif self._file_path.parent.exists():
      assert self._file_path.parent.is_dir(), f"{self._file_path.parent} is not a directory"
      offsets = list(self._search_offsets(content_length))
      offsets.sort()
    else:
      self._file_path.parent.mkdir(parents=True, exist_ok=True)

    if offsets:
      descriptions = []
      for i, offset in enumerate(offsets):
        length: int
        if i == len(offsets) - 1:
          length = content_length - offset
        else:
          length = offsets[i + 1] - offset
        descriptions.append(SegmentDescription(
          offset=offset,
          length=length,
          completed_length=0,
        ))

    return Serial(
      length=content_length,
      min_segment_length=self._min_segment_length,
      descriptions=descriptions,
    )

  def _search_offsets(self, length: int):
    wanna_tail = f"{self._file_path.suffix[1:]}{DOWNLOADING_SUFFIX}"
    file_stem = glob.escape(self._file_path.stem) # maybe include "*" signs

    for matched_path in self._file_path.parent.glob(f"{file_stem}*"):
      matched_tail = matched_path.name[len(self._file_path.stem):]
      if matched_tail == DOWNLOADING_SUFFIX:
        yield 0
      else:
        parts = matched_tail.split(".", maxsplit=2)
        if len(parts) == 3 and parts[0] == "":
          _, str_offset, tail = parts
          if tail == wanna_tail:
            offset: int = -1
            try:
              offset = int(str_offset)
            except ValueError:
              pass
            if 0 < offset < length:
              yield offset

  @property
  def serial(self) -> Serial:
    return self._serial

  def download_segment(self, segment: Segment) -> None:
    chunk_path = self._file_path.parent / chunk_name(self._file_path, segment.offset)
    chunk_size = chunk_path.stat().st_size

    if chunk_size >= segment.length:
      trim_size = chunk_size - segment.length
      if trim_size > 0:
        self._trim_file_tail(chunk_path, trim_size)
      segment.complete()
    else:
      def on_know_support_range() -> None:
        with self._padding_lock:
          self._did_support_range = True
        self._support_range_event.set()

      # 服务器可能变卦，在 meta 中声明支持 Range，但真正 fetch 时又不支持了
      # 只有发起一次真正的 GET 请求，然后从 Response Head 中读取信息才能知道到底值不支持
      # 因此先让第一个 Request 发起请求，并同时阻塞其他任务，直到第一个请求的 HEAD 部分返回再进行下一步操作
      know_support_range: Callable[[], None] | None = None
      to_wait_event: Event | None = None
      is_first_fetch: bool = False

      with self._padding_lock:
        if self._is_first_fetch:
          know_support_range = on_know_support_range
          is_first_fetch = self._is_first_fetch
          self._is_first_fetch = False
        elif not self._support_range_event.is_set():
          to_wait_event = self._support_range_event

      if to_wait_event:
        to_wait_event.wait()
        with self._padding_lock:
          if not self._did_support_range:
            raise ValueError("Range not supported by server")

      try:
        self._download_segment_into_file(
          chunk_path=chunk_path,
          segment=segment,
          know_support_range=know_support_range,
        )
      except RangeNotSupportedError as err:
        if is_first_fetch:
          self._support_range_event.set()
        raise err

  def _trim_file_tail(self, file_path: Path, bytes: int) -> None:
    with open(file_path, "rb+") as file:
      file.seek(0, 2)
      size = file.tell()
      if size >= bytes:
        file.truncate(size - bytes)

  def _download_segment_into_file(self, chunk_path: Path, segment: Segment, know_support_range: Callable[[], None] | None) -> None:
    segment_end = segment.offset + segment.length - 1
    headers: Mapping[str, str | bytes | None] = {**self._headers} if self._headers else {}
    headers["Range"] = f"{segment.offset}-{segment_end}"
    resp = requests.Session().get(
      stream=True,
      url=self._url,
      headers=headers,
      cookies=self._cookies,
      timeout=self._timeout,
    )
    if resp.status_code in (408, 429, 502, 503, 504):
      raise CanRetryError(f"HTTP {resp.status_code} - {resp.reason}")
    resp.raise_for_status()

    content_range = resp.headers.get("Content-Range")
    content_length = resp.headers.get("Content-Length")

    if content_range != f"bytes {segment.offset}-{segment_end}/{segment.length}":
      raise RangeNotSupportedError()
    if content_length != f"{segment.length}":
      raise RangeNotSupportedError()

    if know_support_range:
      know_support_range()

    with open(chunk_path, "wb") as file:
      for chunk in resp.iter_content(self._once_fetch_size):
        chunk_size = len(chunk)
        writable_size = segment.lock(chunk_size)
        if writable_size == 0:
          break
        if writable_size == chunk_size:
          file.write(chunk)
        else:
          file.write(chunk[:writable_size])
        segment.submit(writable_size)

    if not segment.is_completed:
      raise CanRetryError("Connection closed before completing segment")