import time
import shutil
import hashlib
import random
import subprocess
import unittest

from pathlib import Path
from typing import Callable
from threading import Thread

from tests.start_flask import PORT
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
    group = self._create_files_group([
      (f"/images/{file}", str(temp_path / file))
      for file in files
    ])
    while True:
      executor = group.pop_downloading_executor()
      if not executor:
        break
      executor()
      group.raise_if_failure()

  def _create_files_group(self, tasks: list[tuple[str, str]]) -> FilesGroup:
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
      failure_ladder= (5, 5),
      min_segment_length=10240,
      once_fetch_size=2048,
      skip_existing=False,
      timeout=1.5,
      retry=Retry(
        retry_times=0,
        retry_sleep=0,
      ),
    )

  def _temp_path(self, name: str) -> Path:
    path = _TEMP_PATH / name
    path.mkdir(parents=True, exist_ok=True)
    return path