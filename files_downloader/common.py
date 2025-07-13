from pathlib import Path


DOWNLOADING_SUFFIX = ".downloading"

class InterruptionError(Exception):
  def __init__(self) -> None:
    super().__init__("Interrupted")

class CanRetryError(Exception):
  pass

def chunk_name(path: Path, offset: int) -> str:
  if offset == 0:
    return f"{path.name}{DOWNLOADING_SUFFIX}"
  else:
    return f"{path.stem}.{offset}{path.suffix}{DOWNLOADING_SUFFIX}"