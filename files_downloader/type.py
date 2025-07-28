from dataclasses import dataclass
from typing import Mapping, MutableMapping
from os import PathLike


@dataclass
class Task:
  url: str
  file: str | PathLike
  headers: Mapping[str, str | bytes | None] | None = None
  cookies: MutableMapping[str, str] | None = None

class TaskError(Exception):
  def __init__(self, task: Task) -> None:
    super().__init__(f"URL download failed: {task.url}")
    self._task: Task = task

  @property
  def task(self) -> Task:
    return self._task