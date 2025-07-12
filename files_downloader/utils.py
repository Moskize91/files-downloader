from pathlib import Path
from shutil import rmtree


def clean_path(path: Path) -> None:
  if not path.exists():
    return
  if path.is_file():
    path.unlink()
  else:
    rmtree(path)
