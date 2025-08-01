from dataclasses import dataclass
from typing import Callable, Mapping, MutableMapping
from os import PathLike


@dataclass
class Task:
  url: str | Callable[[], str]
  file: str | PathLike
  headers: Mapping[str, str | bytes | None] | None = None
  cookies: MutableMapping[str, str] | None = None

  def get_url(self) -> str:
    if callable(self.url):
      self.url = self.url()
    return self.url

class TaskError(Exception):
  def __init__(self, task: Task, case_error: Exception) -> None:
    super().__init__(f"URL download failed: {task.url}")
    self._task: Task = task
    self._case_error: Exception = case_error

  @property
  def task(self) -> Task:
    return self._task

  @property
  def case_error(self) -> Exception:
    return self._case_error

class RetryError(TaskError):
  pass

class TooManyRetriesError(TaskError):
  pass

class FileDownloadError(TaskError):
  pass