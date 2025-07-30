import time
import shutil
import hashlib
import subprocess
import traceback
import unittest

from pathlib import Path
from threading import Thread, Lock
from parameterized import parameterized

from tests.start_flask import PORT
from downloaderx.type import Task, RetryError
from downloaderx.common import Retry
from downloaderx.files_group import FilesGroup, FileDownloadError
from downloaderx.invoker import download

_TEMP_PATH = Path(__file__).parent / "temp" / "files_group"

class TestFilesGroup(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    cls.process = subprocess.Popen(
      ["python", str(Path(__file__).parent / "start_flask.py")],
      stdout=subprocess.PIPE,
      stderr=subprocess.PIPE,
      text=True
    )
    time.sleep(1.0)
    shutil.rmtree(_TEMP_PATH, ignore_errors=True)

  @classmethod
  def tearDownClass(cls):
    cls.process.terminate()

  def test_download_all_files(self):
    temp_path = self._temp_path("test_download_all_files")
    files = ["alfred.jpg", "mirai.jpg", "mysteries.jpg", "retrato.jpg"]
    retry_errors: list[RetryError] = []
    tasks: list[tuple[str, str]] = []
    for file in files:
      downloaded_path = str(temp_path / file)
      url = f"/images/{file}"
      tasks.append((url, downloaded_path))

    completed_tasks: list[Task] = []
    group = self._create_files_group(
      tasks=tasks,
      completed_tasks=completed_tasks,
      retry_errors=retry_errors,
    )
    while True:
      executor = group.pop_downloading_executor()
      if not executor:
        break
      executor()
      group.raise_if_failure()

    self.assertListEqual(retry_errors, [])
    self.assertTrue(group.is_empty)
    self.assertListEqual(
      [Path(task.file).name for task in completed_tasks],
      files,
    )
    for (_, downloaded_path), file in zip(tasks, files):
      self.assertEqual(
        _sha256(Path(downloaded_path)),
        _sha256(Path(__file__).parent / "assets" / file),
      )

  @parameterized.expand([
    ("", False, False),
    ("_retry", True, False),
    ("_break", True, True),
  ])
  def test_call_invoker_download(self, tail: str, retry: bool, break_random: bool):
    temp_path = self._temp_path("test_call_invoker_download" + tail)
    files = ["alfred.jpg", "mirai.jpg", "mysteries.jpg", "retrato.jpg"]
    tasks: list[Task] = []

    for file in files:
      url = f"/images/{file}"
      query: list[str] = []
      if retry:
        query.append("range=true")
      else:
        query.append("reject_first=true")
      if break_random:
        query.append("break_random=true")
      if query:
        url += "?" + "&".join(query)

      tasks.append(Task(
        url=f"http://localhost:{PORT}{url}",
        file=temp_path / file,
        headers=None,
        cookies=None,
      ))

    download(
      tasks_iter=iter(tasks),
      threads_count=4,
      window_width=2,
      failure_ladder= (50, 50),
      min_segment_length=9216,
      once_fetch_size=2048,
    )

  @parameterized.expand([
    ("", False, False),
    ("_retry", True, False),
    ("_break", True, True),
  ])
  def test_download_in_background(self, tail: str, retry: bool, break_random: bool):
    temp_path = self._temp_path("test_download_in_background" + tail)
    files = ["alfred.jpg", "mirai.jpg", "mysteries.jpg", "retrato.jpg"]
    retry_errors: list[RetryError] = []
    tasks: list[tuple[str, str]] = []
    for file in files:
      downloaded_path = str(temp_path / file)
      url = f"/images/{file}"
      query: list[str] = []
      if retry:
        query.append("range=true")
      else:
        query.append("reject_first=true")
      if break_random:
        query.append("break_random=true")
      if query:
        url += "?" + "&".join(query)
      tasks.append((url, downloaded_path))

    group = self._create_files_group(tasks, retry_errors=retry_errors)
    threads: list[Thread] = []
    raised_error: Exception | None = None

    for _ in range(4):
      def run_in_background(group: FilesGroup):
        nonlocal raised_error
        try:
          while True:
            executor = group.pop_downloading_executor()
            if executor is None:
              break
            executor()
            group.raise_if_failure()
        except Exception as error:
          traceback.print_exc()
          raised_error = error

      thread = Thread(target=run_in_background, args=(group,))
      thread.start()
      threads.append(thread)

    for thread in threads:
      thread.join()

    self.assertIsNone(raised_error)
    self.assertTrue(group.is_empty)

    if not break_random:
      self.assertListEqual(retry_errors, [])

    for (_, downloaded_path), file in zip(tasks, files):
      self.assertEqual(
        _sha256(Path(downloaded_path)),
        _sha256(Path(__file__).parent / "assets" / file),
      )

  def test_download_404_and_dispose(self):
    temp_path = self._temp_path("test_download_404_and_dispose")
    files = ["alfred.jpg", "mirai.jpg", "mysteries.jpg", "404.jpg", "retrato.jpg"]
    tasks: list[tuple[str, str]] = [
      (f"/images/{file}", str(temp_path / file))
      for file in files
    ]
    group = self._create_files_group(tasks)
    threads: list[Thread] = []
    raised_error: Exception | None = None

    for _ in range(4):
      def run_in_background(group: FilesGroup):
        nonlocal raised_error
        try:
          while True:
            executor = group.pop_downloading_executor()
            if executor is None:
              break
            executor()
            group.raise_if_failure()

        except Exception as error:
          raised_error = error
          group.dispose()

      thread = Thread(target=run_in_background, args=(group,))
      thread.start()
      threads.append(thread)

    for thread in threads:
      thread.join()

    self.assertIsInstance(raised_error, FileDownloadError)
    self.assertFalse(group.is_empty)

  def _create_files_group(
        self, tasks: list[tuple[str, str]],
        completed_tasks: list[Task] | None = None,
        retry_errors: list[RetryError] | None = None,
      ) -> FilesGroup:

    list_lock = Lock()

    def on_task_completed(task: Task):
      if completed_tasks is not None:
        with list_lock:
          completed_tasks.append(task)

    def on_task_failed_with_retry_error(error: RetryError):
      if retry_errors is not None:
        with list_lock:
          retry_errors.append(error)

    return FilesGroup(
      tasks_iter = (
        Task(
          url=f"http://localhost:{PORT}{path}",
          file=file,
          headers=None,
          cookies=None,
        )
        for path, file in tasks
      ),
      window_width = 1024,
      failure_ladder= (50, 50),
      min_segment_length=9216,
      once_fetch_size=2048,
      skip_existing=False,
      timeout=1.5,
      on_task_completed=on_task_completed,
      on_task_failed=None,
      on_task_failed_with_retry_error=on_task_failed_with_retry_error,
      retry=Retry(
        retry_times=0,
        retry_sleep=0,
      ),
    )

  def _temp_path(self, name: str) -> Path:
    path = _TEMP_PATH / name
    path.mkdir(parents=True, exist_ok=True)
    return path

def _sha256(file_path: Path) -> str:
  sha256_hash = hashlib.sha256()
  with open(file_path, "rb") as file:
    while True:
      data = file.read(4096)
      if not data:
        break
      sha256_hash.update(data)
  return sha256_hash.hexdigest()