from typing import Iterator, Iterable, Callable
from pathlib import Path
from threading import Lock, Event
from dataclasses import dataclass

from .file import FileDownloader, CanRetryError, InterruptionError
from .type import Task, TaskError, RetryError, TooManyRetriesError, FileDownloadError
from .common import HTTPOptions, Retry
from .utils import list_safe_remove


@dataclass
class _FileNode:
  task: Task
  downloader: FileDownloader
  ladder: int
  current_ladder_failure_count: int
  executors_count: int

# thread safe
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
        on_task_skipped: Callable[[Task], None] | None,
        on_task_failed: Callable[[TaskError], None] | None,
        on_task_failed_with_retry_error: Callable[[RetryError], None] | None,
      ) -> None:

    self._tasks_iter: Iterator[Task] = tasks_iter
    self._window_width: int = window_width
    self._min_segment_length: int = min_segment_length
    self._once_fetch_size: int = once_fetch_size
    self._skip_existing: bool = skip_existing
    self._timeout: float = timeout
    self._retry: Retry = retry
    self._on_task_completed: Callable[[Task], None] = on_task_completed or (lambda _: None)
    self._on_task_skipped: Callable[[Task], None] = on_task_skipped or (lambda _: None)
    self._on_task_failed: Callable[[TaskError], None] | None = on_task_failed
    self._on_task_failed_with_retry_error: Callable[[RetryError], None] = on_task_failed_with_retry_error or (lambda _: None)

    self._lock: Lock = Lock()
    self._did_call_dispose: bool = False
    self._failure_error: TaskError | None = None
    self._failure_ladder: tuple[int, ...] = tuple(failure_ladder)
    self._ladder_nodes: list[list[_FileNode]] = [[]] * len(self._failure_ladder)
    self._maybe_create_new_executors: Event = Event()

    assert len(self._failure_ladder) > 0, "failure ladder must not be empty"
    for ladder_failure_limit in self._failure_ladder:
      assert ladder_failure_limit > 0, "ladder failure limit must be greater than zero"

    self._maybe_create_new_executors.set()

  @property
  def is_empty(self) -> bool:
    with self._lock:
      for ladder_nodes in self._ladder_nodes:
        if ladder_nodes:
          return False
      return True

  def raise_if_failure(self) -> None:
    with self._lock:
      if self._failure_error is not None:
        raise self._failure_error

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

  def wait_windows_update(self) -> None:
    # window 中的 node 被删除了，window 才有可能继续滑动，或者，有一个任务失败了，导致 node 被释放
    # 用户监听到该事件后，可以重新调用 pop_downloading_executor() 试图滑动 window 开始新任务
    self._maybe_create_new_executors.wait()

  def pop_downloading_executor(self) -> Callable[[], None] | None:
    with self._lock:
      if self._failure_error is not None:
        return None
      result = self._node_and_executor()
      if not result:
        self._maybe_create_new_executors.clear()
        return None

    # running in background thread
    def run_downloader_executor(node: _FileNode, executor: Callable[[], None]) -> None:
      try:
        executor()
      except InterruptionError:
        pass

      except CanRetryError as error:
        with self._lock:
          success = self._increase_failure_count(node)
          if success:
            self._on_task_failed_with_retry_error(RetryError(node.task, error))
            self._maybe_create_new_executors.set()
          else:
            self._remove_node(node)
            self._emit_failure_error(TooManyRetriesError(node.task, error))

      except Exception as error:
        with self._lock:
          self._remove_node(node)
          self._emit_failure_error(FileDownloadError(node.task, error))

      finally:
        with self._lock:
          node.executors_count -= 1
          if node.executors_count <= 0:
            completed_path = node.downloader.try_complete()
            if completed_path is not None:
              self._remove_node(node)
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

  def _remove_node(self, node: _FileNode):
    if node.ladder < len(self._ladder_nodes):
      ladder_nodes = self._ladder_nodes[node.ladder]
      removed_node = list_safe_remove(ladder_nodes, node)
      if removed_node is not None:
        self._maybe_create_new_executors.set()

  def _node_and_executor(self) -> tuple[_FileNode, Callable[[], None]] | None:
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
    if task_file_path.exists():
      if self._skip_existing:
        self._on_task_skipped(task)
        return None
      if not task_file_path.is_file():
        raise ValueError(f"Task file path {task_file_path} exists but is not a file.")
      task_file_path.unlink()

    if callable(task.url):
      task.url = task.url()

    http_options = HTTPOptions(
      url=task.url,
      timeout=self._timeout,
      retry=self._retry,
      headers=task.headers,
      cookies=task.cookies,
    )
    try:
      downloader = FileDownloader(
        file_path=task_file_path,
        http_options=http_options,
        min_segment_length=self._min_segment_length,
        once_fetch_size=self._once_fetch_size,
      )
    except Exception as error:
      raise FileDownloadError(task, error)

    return _FileNode(
      task=task,
      downloader=downloader,
      ladder=0,
      current_ladder_failure_count=0,
      executors_count=0,
    )

  def _emit_failure_error(self, error: TaskError) -> None:
    if self._on_task_failed:
      self._on_task_failed(error)
    else:
      self._failure_error = error
