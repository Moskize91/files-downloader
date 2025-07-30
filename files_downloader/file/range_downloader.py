import glob
import requests

from pathlib import Path
from typing import Mapping

from ..common import is_exception_can_retry, CAN_RETRY_STATUS_CODES, Retry, HTTPOptions
from .segment import Serial, Segment, SegmentDescription
from .value_signal import ValueSignal
from .common import chunk_name, DOWNLOADING_SUFFIX
from .errors import CanRetryError, RangeNotSupportedError
from .utils import clean_path


# thread safe
class RangeDownloader:
  def __init__(
        self,
        file_path: Path,
        http_options: HTTPOptions,
        min_segment_length: int,
        once_fetch_size: int,
        excepted_etag: str | None = None,
      ) -> None:

    self._file_path: Path = file_path
    self._http_options: HTTPOptions = http_options
    self._min_segment_length: int = min_segment_length
    self._once_fetch_size: int = once_fetch_size
    self._support_range_signal: ValueSignal[bool] = ValueSignal()

    content_length, etag, range_useable = self._fetch_meta(http_options.retry)
    if content_length is None:
      raise ValueError("Content-Length header is missing in response")
    if not range_useable:
      raise RangeNotSupportedError("Server does not support Range requests")

    self._serial: Serial = self._create_serial(
      content_length=content_length,
      etag=etag,
      excepted_etag=excepted_etag,
    )

  def _fetch_meta(self, retry: Retry):
    resp = retry.request(
      request=lambda: requests.head(
        url=self._http_options.url,
        headers=self._http_options.headers,
        cookies=self._http_options.cookies,
        timeout=self._http_options.timeout,
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
    chunk_size = 0
    if chunk_path.exists():
      chunk_size = chunk_path.stat().st_size

    if chunk_size >= segment.length:
      trim_size = chunk_size - segment.length
      if trim_size > 0:
        self._trim_file_tail(chunk_path, trim_size)
      segment.complete()
      return

    # 服务器可能变卦，在 meta 中声明支持 Range，但真正 fetch 时又不支持了
    # 只有发起一次真正的 GET 请求，然后从 Response Head 中读取信息才能知道到底值不支持
    # 因此先让第一个 Request 发起请求，并同时阻塞其他任务，直到第一个请求的 HEAD 部分返回再进行下一步操作
    with self._support_range_signal.context() as support_range_context:
      if support_range_context.value == False:  # noqa: E712
        raise RangeNotSupportedError(
          message="Task is canceled because server rejects Range request",
          is_canceled_by=True,
        )
      try:
        response = self._create_download_segment_response(segment)
      except RangeNotSupportedError as error:
        if not error.is_canceled_by:
          support_range_context.update_value(False)
        raise error

      support_range_context.update_value(True)
      self._download_segment_into_file(response, chunk_path, segment)

  def _trim_file_tail(self, file_path: Path, bytes: int) -> None:
    with open(file_path, "rb+") as file:
      file.seek(0, 2)
      size = file.tell()
      if size >= bytes:
        file.truncate(size - bytes)

  def _create_download_segment_response(self, segment: Segment):
    download_start = segment.offset + segment.completed_length
    download_end = segment.offset + segment.length - 1
    download_length = download_end - download_start + 1

    headers: Mapping[str, str | bytes | None] = {}
    if self._http_options.headers:
      headers.update(self._http_options.headers)

    headers["Range"] = f"{download_start}-{download_end}"
    response = requests.Session().get(
      stream=True,
      url=self._http_options.url,
      headers=headers,
      cookies=self._http_options.cookies,
      timeout=self._http_options.timeout,
    )
    if response.status_code in CAN_RETRY_STATUS_CODES:
      raise CanRetryError(f"HTTP {response.status_code} - {response.reason}")
    if response.status_code == 416:
      raise RangeNotSupportedError("Server rejects Range request")
    response.raise_for_status()

    if response.status_code == 206:
      content_range = response.headers.get("Content-Range")
      content_length = response.headers.get("Content-Length")

      if content_range != f"bytes {download_start}-{download_end}/{download_length}":
        raise RangeNotSupportedError(f"Unexpected Content-Range: {content_range}")
      if content_length != f"{download_length}":
        raise RangeNotSupportedError(f"Unexpected Content-Length: {content_length}")

    elif response.status_code == 200:
      if download_start != 0:
        raise RangeNotSupportedError("Server returns 200 OK but Range request was made")
      content_length = response.headers.get("Content-Length")
      if content_length != str(download_length):
        raise RangeNotSupportedError(f"Server returns 200 OK but Unexpected Content-Length: {content_length}")

    else:
      raise RangeNotSupportedError(f"Server rejects Range request with status code {response.status_code}")

    return response

  def _download_segment_into_file(self, response: requests.Response, chunk_path: Path, segment: Segment) -> None:
    with open(chunk_path, "ab") as file:
      try:
        for chunk in response.iter_content(self._once_fetch_size):
          chunk_size = len(chunk)
          writable_size = segment.lock(chunk_size)
          if writable_size == 0:
            break
          if writable_size == chunk_size:
            file.write(chunk)
          else:
            file.write(chunk[:writable_size])
          segment.submit(writable_size)

      except Exception as error:
        if is_exception_can_retry(error):
          raise CanRetryError("Download one segment of the file failed") from error
        else:
          raise error

    if not segment.is_completed:
      raise CanRetryError("Connection closed before completing segment")