from typing import Callable
from time import sleep
from requests import exceptions, Response
from .common import CAN_RETRY_STATUS_CODES


class Retry:
  def __init__(
      self,
      retry_times: int,
      retry_sleep: float,
    ) -> None:
    self._retry_times: int = retry_times
    self._retry_sleep: float = retry_sleep

  def request(self, request: Callable[[], Response]) -> Response:
    last_response: Response | None = None
    last_error: Exception | None = None
    for i in range(self._retry_times + 1):
      try:
        last_response = request()
        if last_response.ok:
          return last_response
        if last_response.status_code not in CAN_RETRY_STATUS_CODES:
          break
      except (
        exceptions.ConnectionError,
        exceptions.Timeout,
        exceptions.ProxyError,
      ) as error:
        last_error = error

      if i < self._retry_times:
        sleep(self._retry_sleep)

    if last_error is not None:
      raise last_error

    if last_response is not None:
      last_response.raise_for_status()

    raise RuntimeError("request failed")