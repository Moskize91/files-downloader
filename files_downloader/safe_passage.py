from typing import Generic, TypeVar, Callable
from threading import Lock, Semaphore


D = TypeVar("D")

class SafePassageClosed(Exception):
  pass

class SafePassage(Generic[D]):
  def __init__(self) -> None:
    super().__init__()
    self._main_lock: Lock = Lock()
    self._producer_lock: Lock = Lock()
    self._consumer_lock: Lock = Lock()
    self._sender_semaphore: Semaphore = Semaphore(0)
    self._consumer_can_receive_semaphore: Semaphore = Semaphore(0)
    self._consumer_did_receive_semaphore: Semaphore = Semaphore(0)
    self._buffer: tuple[D] | Exception | None = None
    self._rejected_error: Exception | None = None
    self._did_close: bool = False

  @property
  def did_close(self) -> bool:
    with self._main_lock:
      return self._did_close

  def send(self, data: D) -> None:
    self._run_send((data,))

  def build(self, builder: Callable[[], D]) -> None:
    self._run_send(builder)

  def _run_send(self, target: tuple[D] | Callable[[], D]) -> None:
    with self._producer_lock:
      with self._main_lock:
        self._assert_not_closed()

      self._sender_semaphore.acquire()

      with self._main_lock:
        self._assert_not_closed()
        if self._rejected_error is not None:
          error = self._rejected_error
          self._rejected_error = None
          self._consumer_can_receive_semaphore.release()
          raise error

        if isinstance(target, tuple):
          self._buffer = target
        else:
          try:
            self._buffer = (target(),)
          except Exception as error:
            self._buffer = error
        self._consumer_can_receive_semaphore.release()

      # 此锁用于避免 consumer 没有收到之前， producer 就 close 了。
      self._consumer_did_receive_semaphore.acquire()

  def close(self) -> None:
    with self._producer_lock, self._main_lock:
      if not self._did_close:
        self._did_close = True
        self._sender_semaphore.release()
        self._consumer_can_receive_semaphore.release()

  def receive(self) -> D:
    with self._consumer_lock:
      with self._main_lock:
        self._assert_not_closed()
        self._sender_semaphore.release()

      self._consumer_can_receive_semaphore.acquire()
      try:
        with self._main_lock:
          self._assert_not_closed()
          buffer = self._buffer
          self._buffer = None
          assert buffer is not None
          if isinstance(buffer, Exception):
            raise buffer
          else:
            return buffer[0]

      finally:
        self._consumer_did_receive_semaphore.release()

  def reject(self, error: Exception) -> None:
    with self._consumer_lock:
      with self._main_lock:
        self._assert_not_closed()
        self._rejected_error = error
        self._sender_semaphore.release()
      self._consumer_can_receive_semaphore.acquire()

  def _assert_not_closed(self) -> None:
    if self._did_close:
      raise SafePassageClosed()