import hashlib
import random
import shutil
import subprocess
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from typing import Callable

from downloaderx.common import HTTPOptions, Retry
from downloaderx.file import (
    CanRetryError,
    FileDownloader,
    InterruptionError,
    RangeDownloadFailedError,
)
from downloaderx.file.common import chunk_name
from downloaderx.file.range_downloader import RangeDownloader
from tests.start_flask import PORT

_TEMP_PATH = Path(__file__).parent / "temp" / "file_downloader"


class TestFileDownloader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.process = subprocess.Popen(
            ["python", str(Path(__file__).parent / "start_flask.py")],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
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
        # pylint: disable=protected-access
        self.assertIsNone(file._range_downloader)
        run_download_task = file.pop_downloading_task()
        assert run_download_task is not None
        self.assertIsNone(file.pop_downloading_task())
        run_download_task()

        raw_file = Path(__file__).parent / "assets" / "mirai.jpg"
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
        raw_file = Path(__file__).parent / "assets" / "mirai.jpg"
        download_file = temp_path / "mirai.jpg"
        file = self._create_file(
            path="/images/mirai.jpg?range=true",
            download_file=download_file,
        )
        # pylint: disable=protected-access
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
        raw_file = Path(__file__).parent / "assets" / "mirai.jpg"
        download_file = temp_path / "mirai.jpg"
        file = self._create_file(
            path="/images/mirai.jpg",
            download_file=download_file,
        )
        # pylint: disable=protected-access
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

        for error in errors[1:]:  # 第一个可能直接 200 成功，测试用例中应该排除随机性
            assert isinstance(error, RangeDownloadFailedError), "Expected RangeNotSupportedError"

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
        raw_file = Path(__file__).parent / "assets" / "mirai.jpg"
        download_file = temp_path / "mirai.jpg"
        file = self._create_file(
            path="/images/mirai.jpg?range=true&break_random=true",
            download_file=download_file,
        )
        # pylint: disable=protected-access
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
                assert not isinstance(error, RangeDownloadFailedError), (
                    "Expected CanRetryError, not RangeNotSupportedError"
                )
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
        # pylint: disable=protected-access
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


class TestContentLengthFallback(unittest.TestCase):
    """测试Content-Length缺失时的降级行为"""

    @classmethod
    def setUpClass(cls):
        cls.process = subprocess.Popen(
            ["python", str(Path(__file__).parent / "start_flask.py")],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        time.sleep(1.0)
        shutil.rmtree(_TEMP_PATH, ignore_errors=True)

    @classmethod
    def tearDownClass(cls):
        cls.process.terminate()

    def test_download_without_content_length(self):
        """测试当服务器缺少Content-Length头时降级到非range下载模式"""
        temp_path = _TEMP_PATH / "test_download_without_content_length"
        temp_path.mkdir(parents=True, exist_ok=True)
        download_file = temp_path / "mirai.jpg"

        # 使用 no_content_length=true 参数让服务器不返回Content-Length头
        file = FileDownloader(
            file_path=download_file,
            min_segment_length=1024,
            once_fetch_size=2048,
            http_options=HTTPOptions(
                url=f"http://localhost:{PORT}/images/mirai.jpg?range=true&no_content_length=true",
                timeout=1.5,
                headers=None,
                cookies=None,
                retry=Retry(retry_times=0, retry_sleep=0),
            ),
        )

        # 当Content-Length缺失时，_range_downloader应该为None（降级到非range模式）
        # 因为RangeDownloader在初始化时会抛出RangeDownloadFailedError，被FileDownloader捕获并忽略
        self.assertIsNone(file._range_downloader)  # pylint: disable=protected-access

        # 文件应该能正常下载完成（使用非range模式）
        run_download_task = file.pop_downloading_task()
        self.assertIsNotNone(run_download_task, "应该能获取下载任务")
        self.assertIsNone(file.pop_downloading_task(), "非range模式应该只有一个任务")

        # 执行下载任务
        assert run_download_task is not None, "下载任务不应该为None"
        run_download_task()

        # 在非range模式下，文件应该作为.downloading文件存在
        raw_file = Path(__file__).parent / "assets" / "mirai.jpg"
        chunk_file = download_file.parent / f"{download_file.name}.downloading"
        self.assertTrue(chunk_file.exists(), f"下载块文件应该存在: {chunk_file}")

        # 验证文件完整性
        self.assertEqual(
            _sha256(chunk_file),
            _sha256(raw_file),
        )

        # 完成下载
        final_path = file.try_complete()
        self.assertEqual(final_path, download_file)
        self.assertTrue(download_file.exists(), f"最终下载文件应该存在: {download_file}")
        self.assertEqual(
            _sha256(download_file),
            _sha256(raw_file),
        )


class TestRangeDownloaderResumeOffsets(unittest.TestCase):
    def test_search_offsets_recovers_zero_chunk_for_regular_suffix(self):
        offsets = self._search_offsets_for_existing_chunks(
            file_name="mirai.jpg",
            existing_offsets=[0, 1234],
            content_length=10_000,
        )

        self.assertEqual(offsets, [0, 1234])

    def test_search_offsets_recovers_zero_chunk_for_compressed_suffix(self):
        offsets = self._search_offsets_for_existing_chunks(
            file_name="latest-all-json.bz2",
            existing_offsets=[0, 23_173_786_269, 46_347_572_538],
            content_length=100_000_000_000,
        )

        self.assertEqual(offsets, [0, 23_173_786_269, 46_347_572_538])

    def _search_offsets_for_existing_chunks(
        self,
        file_name: str,
        existing_offsets: list[int],
        content_length: int,
    ) -> list[int]:
        with TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / file_name
            for offset in existing_offsets:
                (file_path.parent / chunk_name(file_path, offset)).write_bytes(b"x")

            range_downloader = RangeDownloader.__new__(RangeDownloader)
            range_downloader._file_path = file_path  # pylint: disable=protected-access
            return sorted(range_downloader._search_offsets(content_length))  # pylint: disable=protected-access
