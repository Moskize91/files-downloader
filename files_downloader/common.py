from pathlib import Path


DOWNLOADING_SUFFIX = ".downloading"
CAN_RETRY_STATUS_CODES = (408, 429, 502, 503, 504)

def chunk_name(path: Path, offset: int) -> str:
  if offset == 0:
    return f"{path.name}{DOWNLOADING_SUFFIX}"
  else:
    return f"{path.stem}.{offset}{path.suffix}{DOWNLOADING_SUFFIX}"