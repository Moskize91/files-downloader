class InterruptionError(Exception):
  def __init__(self) -> None:
    super().__init__("Interrupted")

class CanRetryError(Exception):
  pass