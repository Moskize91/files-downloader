from typing import Callable
from threading import Lock


class Statistics:
  def __init__(
        self,
        total_bytes: int,
        completed_bytes: int,
        on_update: Callable[[int, int], None],
      ) -> None:

    self._total_bytes: int = total_bytes
    self._completed_bytes: int = completed_bytes
    self._on_update: Callable[[int, int], None] = on_update

  @property
  def total_bytes(self) -> int:
    return self._total_bytes

  @property
  def completed_bytes(self) -> int:
    return self._completed_bytes

  def submit_bytes(self, submitted_bytes: int) -> None:
    if submitted_bytes > 0:
      self._completed_bytes += submitted_bytes
      self._on_update(submitted_bytes, 0)

  def cancel(self):
    self._on_update(
      -self._completed_bytes,
      -self._total_bytes,
    )
    self._total_bytes = 0
    self._completed_bytes = 0

class StatisticsHub:
  def __init__(self) -> None:
    self._total_bytes: int = 0
    self._completed_bytes: int = 0
    self._lock: Lock = Lock()

  def create(self, total_bytes: int) -> Statistics:
    return Statistics(
      total_bytes=total_bytes,
      completed_bytes=0,
      on_update=self._on_update,
    )

  def _on_update(self, submitted_bytes: int, total_bytes: int) -> None:
    with self._lock:
      self._completed_bytes += submitted_bytes
      self._total_bytes += total_bytes