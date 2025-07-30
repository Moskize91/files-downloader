import unittest

from threading import Thread
from files_downloader.safe_passage import SafePassage


class TestSafePassage(unittest.TestCase):

  def test_send_data(self):
    passage: SafePassage[int] = SafePassage()
    consumer = _TestConsumer(passage)
    consumer.thread.start()
    sending_data = list(range(100))

    for data in sending_data:
      passage.send(data)
    passage.close()

    self.assertListEqual(
      list1=sending_data,
      list2=consumer.received,
    )

  def test_build_data(self):
    passage: SafePassage[int] = SafePassage()
    consumer = _TestConsumer(passage)
    consumer.thread.start()
    sending_data = list(range(100))

    for data in sending_data:
      passage.build(lambda d=data: d)
    passage.close()

    self.assertListEqual(
      list1=sending_data,
      list2=consumer.received,
    )

  def test_reject_data(self):
    passage: SafePassage[int] = SafePassage()
    built: list[int] = []
    received: list[int] = []

    def build_data() -> int:
      data = len(built) + 1
      built.append(data)
      return data

    def run_consumer():
      while True:
        if len(received) >= 60:
          passage.reject(RuntimeError())
          break
        else:
          data = passage.receive()
          received.append(data)

    thread = Thread(target=run_consumer)
    thread.start()
    broken_error: Exception | None = None

    for _ in range(100):
      try:
        passage.build(build_data)
      except Exception as error:
        broken_error = error
        break

    thread.join()
    self.assertIsInstance(broken_error, RuntimeError)
    self.assertListEqual(built, received)
    self.assertListEqual(built, list(range(1, 60 + 1)))

class _TestConsumer:
  def __init__(self, passage: SafePassage[int]) -> None:
    self.received: list[int] = []
    self.end_error: Exception | None = None
    self.thread = Thread(
      target=self._run_consumer,
      args=(passage,),
    )

  def _run_consumer(self, passage: SafePassage[int]):
    while True:
      try:
        data = passage.receive()
        self.received.append(data)
      except Exception as error:
        self.end_error = error
        break