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
from files_downloader.common import Retry, HTTPOptions
from files_downloader.file import (
  FileDownloader,
  InterruptionError,
  CanRetryError,
  RangeNotSupportedError,
)


_TEMP_PATH = Path(__file__).parent / "temp"

class TestDownload(unittest.TestCase):

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

  def test_download_whole_file(self):
    temp_path = self._temp_path("test_download_whole_file")
    download_file = temp_path / "mirai.jpg"
    file = self._create_file(
      path="/images/mirai.jpg?reject_first=true",
      download_file=download_file,
    )
    self.assertIsNone(file._range_downloader)
    run_download_task = file.pop_downloading_task()
    assert run_download_task is not None
    self.assertIsNone(file.pop_downloading_task())
    run_download_task()

    raw_file = Path(__file__).parent / "mirai.jpg"
    chunk_file = download_file.parent / f"{download_file.name}.downloading"
    self.assertTrue(chunk_file.exists())
    self.assertEqual(
      _sha256(chunk_file),
      _sha256(raw_file),
    )
    final_path = file.try_complete()
    self.assertEqual(final_path, download_file)
    self.assertTrue(download_file.exists())
    self.assertEqual(
      _sha256(download_file),
      _sha256(raw_file),
    )

  def test_download_segments(self):
    temp_path = self._temp_path("test_download_segments")
    raw_file = Path(__file__).parent / "mirai.jpg"
    download_file = temp_path / "mirai.jpg"
    file = self._create_file(
      path="/images/mirai.jpg?range=true",
      download_file=download_file,
    )
    self.assertIsNotNone(file._range_downloader)

    segments_count = 7
    tasks: list[Callable[[], None]] = []

    for i in range(segments_count):
      run_download_task = file.pop_downloading_task()
      assert run_download_task is not None, f"Failed to pop task {i + 1}/{segments_count}"
      tasks.append(run_download_task)

    for i in _shuffle_indexes(segments_count, seed=4399):
      tasks[i]()

    self.assertIsNone(file.pop_downloading_task())
    self.assertEqual(file.try_complete(), download_file)
    self.assertEqual(
      _sha256(download_file),
      _sha256(raw_file),
    )

  def test_download_fake_segments(self):
    temp_path = self._temp_path("test_download_fake_segments")
    raw_file = Path(__file__).parent / "mirai.jpg"
    download_file = temp_path / "mirai.jpg"
    file = self._create_file(
      path="/images/mirai.jpg",
      download_file=download_file,
    )
    self.assertIsNotNone(file._range_downloader)

    segments_count = 5
    threads: list[Thread] = []
    errors: list[Exception | None] = [None] * segments_count

    for i in range(segments_count):
      def invoker(index: int):
        task = file.pop_downloading_task()
        assert task is not None, "Failed to pop task"
        try:
          task()
        except Exception as error:
          errors[index] = error

      thread = Thread(target=invoker, args=(i,))
      thread.start()
      threads.append(thread)

    for thread in threads:
      thread.join()

    canceled_count: int = 0
    for error in errors:
      assert isinstance(error, RangeNotSupportedError), "Expected RangeNotSupportedError"
      if error.is_canceled_by:
        canceled_count += 1
    self.assertEqual(canceled_count, segments_count - 1)

    download_task = file.pop_downloading_task()
    assert download_task is not None, "Failed to pop final task"
    self.assertIsNone(file.pop_downloading_task())

    download_task()
    self.assertEqual(file.try_complete(), download_file)
    self.assertEqual(
      _sha256(download_file),
      _sha256(raw_file),
    )

  def test_retry_download(self):
    temp_path = self._temp_path("test_retry_download")
    raw_file = Path(__file__).parent / "mirai.jpg"
    download_file = temp_path / "mirai.jpg"
    file = self._create_file(
      path="/images/mirai.jpg?range=true&break_random=true",
      download_file=download_file,
    )
    self.assertIsNotNone(file._range_downloader)

    segments_count = 4
    tasks: list[Callable[[], None]] = []

    for i in range(segments_count):
      run_download_task = file.pop_downloading_task()
      assert run_download_task is not None, f"Failed to pop task {i + 1}/{segments_count}"
      tasks.append(run_download_task)

    tasks_queue = [tasks[i] for i in _shuffle_indexes(segments_count, seed=12125)]
    while tasks_queue:
      task = tasks_queue.pop()
      try:
        task()
      except CanRetryError as error:
        assert not isinstance(error, RangeNotSupportedError), "Expected CanRetryError, not RangeNotSupportedError"
        task = file.pop_downloading_task()
        if task:
          tasks_queue.append(task)

    self.assertEqual(file.try_complete(), download_file)
    self.assertEqual(
      _sha256(download_file),
      _sha256(raw_file),
    )

  def test_dispose_download(self):
    temp_path = self._temp_path("test_dispose_download")
    download_file = temp_path / "mirai.jpg"
    file = self._create_file(
      path="/images/mirai.jpg?range=true",
      download_file=download_file,
    )
    self.assertIsNotNone(file._range_downloader)

    group_size = 3
    to_cancel_threads: list[Thread] = []
    errors: list[Exception | None] = [None] * group_size

    def run_task(task: Callable[[], None], index: int) -> None:
      try:
        task()
      except Exception as error:
        errors[index] = error

    for i in range(group_size):
      task = file.pop_downloading_task()
      assert task is not None, "Failed to pop task"
      to_cancel_threads.append(Thread(target=run_task, args=(task, i)))

    for _ in range(group_size):
      task = file.pop_downloading_task()
      assert task is not None, "Failed to pop task"
      task()

    for thread in to_cancel_threads:
      thread.start()

    file.dispose()

    for thread in to_cancel_threads:
      thread.join()

    for error in errors:
      self.assertIsInstance(error, InterruptionError)

    self.assertIsNone(file.pop_downloading_task())

    chunk_file_count: int = 0
    for file in temp_path.iterdir():
      if file.is_file() and file.name.endswith(".downloading"):
        chunk_file_count += 1

    self.assertEqual(chunk_file_count, group_size * 2)

  def _temp_path(self, name: str) -> Path:
    path = _TEMP_PATH / name
    path.mkdir(parents=True, exist_ok=True)
    return path

  def _create_file(self, path: str, download_file: Path) -> FileDownloader:
    return FileDownloader(
      file_path=download_file,
      min_segment_length=1024,
      once_fetch_size=2048,
      http_options=HTTPOptions(
        url=f"http://localhost:{PORT}{path}",
        timeout=1.5,
        headers=None,
        cookies=None,
        retry=Retry(
          retry_times=0,
          retry_sleep=0,
        ),
      ),
    )

def _sha256(file_path: Path) -> str:
  sha256_hash = hashlib.sha256()
  with open(file_path, "rb") as file:
    while True:
      data = file.read(4096)
      if not data:
        break
      sha256_hash.update(data)
  return sha256_hash.hexdigest()

def _shuffle_indexes(length: int, seed: int) -> list[int]:
  random.seed(seed)
  indexes = list(range(length))
  random.shuffle(indexes)
  return indexes