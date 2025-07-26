import requests.exceptions
from pathlib import Path


DOWNLOADING_SUFFIX = ".downloading"
CAN_RETRY_STATUS_CODES = (408, 429, 502, 503, 504)

def chunk_name(path: Path, offset: int) -> str:
  if offset == 0:
    return f"{path.name}{DOWNLOADING_SUFFIX}"
  else:
    return f"{path.stem}.{offset}{path.suffix}{DOWNLOADING_SUFFIX}"

_CAN_RETRY_EXCEPTIONS = (
  requests.exceptions.ConnectionError,
  requests.exceptions.Timeout,
  requests.exceptions.ProxyError,
  requests.exceptions.SSLError,
  requests.exceptions.ChunkedEncodingError,
  requests.exceptions.ReadTimeout,
  requests.exceptions.ConnectTimeout,
  requests.exceptions.TooManyRedirects,
)

def is_exception_can_retry(error: Exception) -> bool:
  for retry_class in _CAN_RETRY_EXCEPTIONS:
    if isinstance(error, retry_class):
      return True
  return False