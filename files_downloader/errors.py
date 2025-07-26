class InterruptionError(Exception):
  def __init__(self) -> None:
    super().__init__("Interrupted")

class CanRetryError(Exception):
  pass

class RangeNotSupportedError(CanRetryError):
  def __init__(self, message: str, is_canceled_by: bool = False) -> None:
    super().__init__(message)
    self._is_canceled_by: bool = is_canceled_by

  @property
  def is_canceled_by(self) -> bool:
    return self._is_canceled_by