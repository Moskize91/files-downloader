from threading import Lock, Semaphore
from typing import Generic, TypeVar, Callable


V = TypeVar("V")

class ValueContext(Generic[V]):
  def __init__(
      self,
      value: V | None,
      on_exit: Callable[[], None] | None,
      on_set_value: Callable[[V], None] | None,
    ) -> None:
    super().__init__()
    self._value: V | None = value
    self._on_exit: Callable[[], None] | None = on_exit
    self._on_set_value: Callable[[V], None] | None = on_set_value

  @property
  def value(self) -> V | None:
    return self._value

  def update_value(self, value: V) -> None:
    if self._on_set_value is not None:
      assert value is not None, "value must not be None"
      self._value = value
      self._on_set_value(value)

  def __enter__(self) -> "ValueContext[V]":
    return self

  def __exit__(self, exc_type, exc_value, traceback) -> None:
    if self._on_exit is not None:
      self._on_exit()

class ValueSignal(Generic[V]):
  def __init__(self) -> None:
    super().__init__()
    self._value: V | None = None
    self._semaphore: Semaphore = Semaphore(1)
    self._lock: Lock = Lock()
    self._acquire_count: int = 0

  def context(self) -> ValueContext[V]:
    with self._lock:
      if self._value is None:
        self._acquire_count += 1
      else:
        return ValueContext(
          value=self._value,
          on_exit=None,
          on_set_value=None,
        )
    self._semaphore.acquire()

    return ValueContext(
      value=self._value,
      on_exit=self._exit_context,
      on_set_value=self._set_value,
    )

  def _set_value(self, value: V) -> None:
    acquire_count: int
    with self._lock:
      self._value = value
      acquire_count = self._acquire_count

    for _ in range(acquire_count):
      self._semaphore.release()

  def _exit_context(self) -> None:
    with self._lock:
      if self._value is not None:
        return
      self._acquire_count -= 1
    self._semaphore.release()