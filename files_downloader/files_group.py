from typing import Iterator, Iterable, Callable
from pathlib import Path
from threading import Lock
from dataclasses import dataclass

from .file import FileDownloader, CanRetryError, InterruptionError
from .type import Task, TaskError
from .common import HTTPOptions, Retry
from .utils import list_safe_remove


@dataclass
class _FileNode:
  task: Task
  downloader: FileDownloader
  ladder: int
  current_ladder_failure_count: int
  executors_count: int

class FileDownloadError(Exception):
  def __init__(self, case_error: Exception) -> None:
    super().__init__()
    self.case_error: Exception = case_error

class FilesGroup:
  def __init__(
        self,
        tasks_iter: Iterator[Task],
        window_width: int,
        failure_ladder: Iterable[int],
        min_segment_length: int,
        once_fetch_size: int,
        skip_existing: bool,
        timeout: float,
        retry: Retry,
        on_task_completed: Callable[[Task], None] | None,
        on_task_failed_with_retry_error: Callable[[Task, CanRetryError], None] | None
      ) -> None:

    self._tasks_iter: Iterator[Task] = tasks_iter
    self._window_width: int = window_width
    self._min_segment_length: int = min_segment_length
    self._once_fetch_size: int = once_fetch_size
    self._skip_existing: bool = skip_existing
    self._timeout: float = timeout
    self._retry: Retry = retry
    self._on_task_completed: Callable[[Task], None] = on_task_completed or (lambda _: None)
    self._on_task_failed_with_retry_error: Callable[[Task, CanRetryError], None] = on_task_failed_with_retry_error or (lambda _, __: None)

    self._lock: Lock = Lock()
    self._did_call_dispose: bool = False
    self._failure_signal: tuple[Task, Exception] | None = None
    self._failure_ladder: tuple[int, ...] = tuple(failure_ladder)
    self._ladder_nodes: list[list[_FileNode]] = [[]] * len(self._failure_ladder)

    assert len(self._failure_ladder) > 0, "failure ladder must not be empty"
    for ladder_failure_limit in self._failure_ladder:
      assert ladder_failure_limit > 0, "ladder failure limit must be greater than zero"

  def raise_if_failure(self) -> None:
    with self._lock:
      if self._failure_signal is not None:
        task, error = self._failure_signal
        if isinstance(error, CanRetryError):
          raise TaskError(task) from error
        else:
          raise FileDownloadError(error)

  def dispose(self) -> None:
    downloaders: list[FileDownloader] = []
    with self._lock:
      if self._did_call_dispose:
        return
      self._did_call_dispose = True
      for ladder_nodes in self._ladder_nodes:
        for node in ladder_nodes:
          downloaders.append(node.downloader)

    for downloader in downloaders:
      downloader.dispose()

  def pop_downloading_executor(self) -> Callable[[], None] | None:
    with self._lock:
      if self._failure_signal is not None:
        return None
      result = self._node_and_executor()
      if not result:
        return None

    # running in background thread
    def run_downloader_executor(node: _FileNode, executor: Callable[[], None]) -> None:
      try:
        executor()
      except InterruptionError:
        pass

      except CanRetryError as error:
        with self._lock:
          success = False
          if self._failure_signal is None:
            success = self._increase_failure_count(node)
            if not success:
              self._failure_signal = (node.task, error)
          if success:
            self._on_task_failed_with_retry_error(node.task, error)
          else:
            list_safe_remove(self._ladder_nodes[node.ladder], node)

      except Exception as error:
        with self._lock:
          if self._failure_signal is None:
            self._failure_signal = (node.task, error)
          list_safe_remove(self._ladder_nodes[node.ladder], node)

      finally:
        with self._lock:
          node.executors_count -= 1
          if node.executors_count <= 0:
            completed_path = node.downloader.try_complete()
            if completed_path is not None:
              list_safe_remove(self._ladder_nodes[node.ladder], node)
              self._on_task_completed(node.task)

    node, executor = result
    node.executors_count += 1

    return lambda: run_downloader_executor(node, executor)

  def _increase_failure_count(self, node: _FileNode) -> bool:
    ladder_failure_limit = self._failure_ladder[node.ladder]
    origin_ladder_nodes = self._ladder_nodes[node.ladder]
    if node in origin_ladder_nodes:
      node.current_ladder_failure_count += 1
      if node.current_ladder_failure_count >= ladder_failure_limit:
        origin_ladder_nodes.remove(node)
        node.ladder += 1
        node.current_ladder_failure_count = 0
        if node.ladder >= len(self._ladder_nodes):
          return False
        self._ladder_nodes[node.ladder].append(node)
    return True

  def _node_and_executor(self) -> tuple[_FileNode, Callable[[], None]] | None:
    # TODO: 重新审视这段逻辑，在 downloader 调用 pop 后，应该 try_complete 然后将成功的删除
    window_nodes = self._ladder_nodes[0]
    for node in window_nodes:
      executor = node.downloader.pop_downloading_task()
      if executor:
        return node, executor

    while len(window_nodes) < self._window_width:
      task = next(self._tasks_iter, None)
      if not task:
        break
      node = self._create_file_node(task)
      if node:
        window_nodes.append(node)
        executor = node.downloader.pop_downloading_task()
        if executor:
          return node, executor

    for i in range(1, len(self._ladder_nodes)):
      ladder_nodes = self._ladder_nodes[i]
      for node in ladder_nodes:
        executor = node.downloader.pop_downloading_task()
        if executor:
          return node, executor

    return None

  def _create_file_node(self, task: Task) -> _FileNode | None:
    task_file_path = Path(task.file)
    if self._skip_existing and task_file_path.exists():
      return None

    http_options = HTTPOptions(
      url=task.url,
      timeout=self._timeout,
      retry=self._retry,
      headers=task.headers,
      cookies=task.cookies,
    )
    downloader = FileDownloader(
      file_path=task_file_path,
      http_options=http_options,
      min_segment_length=self._min_segment_length,
      once_fetch_size=self._once_fetch_size,
    )
    return _FileNode(
      task=task,
      downloader=downloader,
      ladder=0,
      current_ladder_failure_count=0,
      executors_count=0,
    )