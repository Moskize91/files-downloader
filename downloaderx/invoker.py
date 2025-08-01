from typing import Iterator, Iterable, Callable
from threading import Thread, Lock

from .files_group import FilesGroup
from .safe_passage import SafePassage, SafePassageClosed
from .type import TaskError, RetryError
from .type import Task
from .common import Retry


def download(
      tasks_iter: Task | Iterator[Task],
      window_width: int = 7,
      threads_count: int = 1,
      failure_ladder: int | Iterable[int] = 5,
      min_segment_length: int = 1024 * 1024, # 1 MiB
      once_fetch_size: int = 1024 * 16,
      timeout: float = 30.0,
      retry_times: int = 0,
      retry_sleep: float = 0.0,
      override_existing_files: bool = False,
      on_task_completed: Callable[[Task], None] | None = None,
      on_task_skipped: Callable[[Task], None] | None = None,
      on_task_failed: Callable[[TaskError], None] | None = None,
      on_task_failed_with_retry_error: Callable[[RetryError], None] | None = None,
    ) -> None:

  assert threads_count > 0, "threads_count must be greater than zero"

  if isinstance(tasks_iter, Task):
    tasks_iter = iter([tasks_iter])

  if isinstance(failure_ladder, int):
    failure_ladder = (failure_ladder,)

  group = FilesGroup(
    tasks_iter=tasks_iter,
    window_width=window_width,
    failure_ladder=failure_ladder,
    min_segment_length=min_segment_length,
    once_fetch_size=once_fetch_size,
    skip_existing=not override_existing_files,
    timeout=timeout,
    on_task_completed=on_task_completed,
    on_task_skipped=on_task_skipped,
    on_task_failed=on_task_failed,
    on_task_failed_with_retry_error=on_task_failed_with_retry_error,
    retry=Retry(
      retry_times=retry_times,
      retry_sleep=retry_sleep,
    ),
  )
  threads: list[Thread] = []
  invoker = _Invoker(
    group=group,
    threads_count=threads_count,
    on_task_failed=on_task_failed,
  )
  for _ in range(threads_count):
    thread = Thread(target=invoker.run_job)
    thread.start()
    threads.append(thread)

  try:
    invoker.run_main()

  finally:
    for thread in threads:
      thread.join()

class _Invoker:
  def __init__(
        self, group: FilesGroup,
        threads_count: int,
        on_task_failed: Callable[[TaskError], None] | None,
      ) -> None:

    self._group: FilesGroup = group
    self._threads_count: int = threads_count
    self._on_task_failed: Callable[[TaskError], None] | None = on_task_failed
    self._passage: SafePassage[Callable[[], None] | None] = SafePassage()
    self._failure_lock: Lock = Lock()
    self._failure_mark: bool = False

  def run_main(self) -> None:
    found_no_more = False
    found_error: Exception | None = None

    def build_executor() -> Callable[[], None] | None:
      nonlocal found_no_more, found_error
      found_no_more = False
      found_error = None
      try:
        executor = self._group.pop_downloading_executor()
        found_no_more = (executor is None)
        return executor
      except Exception as error:
        found_error = error
        return None

    try:
      while True:
        while True:
          self._passage.build(build_executor)
          if found_error is not None:
            if self._on_task_failed and isinstance(found_error, TaskError):
              self._on_task_failed(found_error)
            else:
              raise found_error
          if found_no_more:
            break

        if self._group.is_empty:
          break
        self._group.wait_windows_update()

    except Exception as error:
      self._group.dispose()
      raise error

    finally:
      self._passage.close()

  def run_job(self):
    try:
      while True:
        executor = self._passage.receive()
        if executor:
          executor()
          self._group.raise_if_failure()

    except SafePassageClosed:
      pass

    except Exception as error:
      with self._failure_lock:
        if not self._failure_mark:
          self._failure_mark = True
          self._passage.reject(error)