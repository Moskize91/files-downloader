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
from files_downloader.file import CanRetryError
from files_downloader.type import Task
from files_downloader.common import Retry
from files_downloader.files_group import FilesGroup

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
    retry_errors: list[CanRetryError] = []
    tasks: list[tuple[str, str]] = []
    for file in files:
      downloaded_path = str(temp_path / file)
      url = f"/images/{file}"
      tasks.append((url, downloaded_path))

    group = self._create_files_group(tasks, retry_errors)
    while True:
      executor = group.pop_downloading_executor()
      if not executor:
        break
      executor()
      group.raise_if_failure()

    self.assertListEqual(retry_errors, [])
    self.assertTrue(group.is_empty)

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
  def test_download_in_background(self, tail: str, retry: bool, break_random: bool):
    temp_path = self._temp_path("test_download_in_background" + tail)
    files = ["alfred.jpg", "mirai.jpg", "mysteries.jpg", "retrato.jpg"]
    retry_errors: list[CanRetryError] = []
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

    group = self._create_files_group(tasks, retry_errors)
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

      thread = Thread(target=lambda: run_in_background(group))
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

  def _create_files_group(
        self, tasks: list[tuple[str, str]],
        retry_errors: list[CanRetryError] | None = None,
      ) -> FilesGroup:

    retry_errors_lock = Lock()
    if retry_errors is None:
      retry_errors = []

    def on_catch_error(_: Task, error: CanRetryError):
      with retry_errors_lock:
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
      on_task_completed=None,
      on_task_failed_with_retry_error=on_catch_error,
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