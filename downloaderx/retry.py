import http.client
import socket
import ssl
from time import sleep
from typing import Callable

import requests
import urllib3.exceptions
from requests import Response

CAN_RETRY_STATUS_CODES = (408, 429, 502, 503, 504)

_CAN_RETRY_EXCEPTIONS = (
    # requests 层异常
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ProxyError,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ReadTimeout,
    requests.exceptions.ConnectTimeout,
    requests.exceptions.TooManyRedirects,
    requests.exceptions.ContentDecodingError,  # 内容解码错误（gzip等）
    requests.exceptions.HTTPError,  # HTTP 错误状态码
    # urllib3 层异常（重要！可能直接抛出）
    urllib3.exceptions.ConnectionError,
    urllib3.exceptions.TimeoutError,
    urllib3.exceptions.ReadTimeoutError,
    urllib3.exceptions.ConnectTimeoutError,
    urllib3.exceptions.NewConnectionError,
    urllib3.exceptions.ProxyError,
    urllib3.exceptions.ProtocolError,
    urllib3.exceptions.MaxRetryError,
    urllib3.exceptions.ClosedPoolError,
    urllib3.exceptions.EmptyPoolError,
    urllib3.exceptions.HostChangedError,  # 主机变更错误
    urllib3.exceptions.DecodeError,  # 内容解码错误
    urllib3.exceptions.ResponseError,  # 响应错误
    #
    # SSL异常处理参考文档：
    # - Python SSL官方文档: https://docs.python.org/3/library/ssl.html
    # - SSL异常详解: https://medium.com/@obaff/possible-causes-and-solutions-for-sslerror-ssleoferror-8-eof-occurred-in-violation-of-protocol-9ad28ff56d56
    # - urllib3 SSLEOFError行为变化: https://github.com/urllib3/urllib3/issues/3100
    # - requests SSLZeroReturnError问题: https://github.com/psf/requests/issues/6429
    #
    # SSL异常层次结构：
    #     OSError
    #      └── ssl.SSLError (基类 - 不应直接捕获，应捕获具体子类)
    #           ├── ssl.SSLZeroReturnError - SSL连接已干净关闭 (可重试)
    #           ├── ssl.SSLWantReadError - 非阻塞socket需要读 (可重试，但requests中不应出现)
    #           ├── ssl.SSLWantWriteError - 非阻塞socket需要写 (可重试，但requests中不应出现)
    #           ├── ssl.SSLSyscallError - 系统错误 (可重试)
    #           ├── ssl.SSLEOFError - SSL连接被意外关闭 (可重试)
    #           └── ssl.SSLCertVerificationError - 证书验证失败 (不可重试!)
    #
    # 标准库 SSL 层异常（最底层，最关键！）
    # 注意：不包含 ssl.SSLError 基类，避免捕获不可重试的 ssl.SSLCertVerificationError
    # 注意：不包含 requests.exceptions.SSLError 和 urllib3.exceptions.SSLError
    #       因为它们是包装类，会包装所有底层SSL异常（包括不可重试的证书验证失败）
    #       正确做法：只捕获具体的、可重试的 ssl.* 子类，通过异常链遍历来识别
    ssl.SSLEOFError,  # SSL连接被意外关闭 (UNEXPECTED_EOF_WHILE_READING)
    ssl.SSLZeroReturnError,  # SSL连接已干净关闭（连接池复用时常见）
    ssl.SSLSyscallError,  # SSL系统调用错误（底层socket/系统问题）
    ssl.SSLWantReadError,  # 非阻塞socket需要读（理论上requests中不应出现）
    ssl.SSLWantWriteError,  # 非阻塞socket需要写（理论上requests中不应出现）
    # 标准库 socket 层异常
    socket.timeout,  # 套接字超时
    socket.error,  # 套接字错误
    # Python 内置异常
    ConnectionError,  # Python 3.3+ 连接错误
    TimeoutError,  # Python 3.3+ 超时错误
    OSError,  # 底层 I/O 错误 (包含连接重置)
    IOError,  # I/O 错误（兼容性考虑）
    # HTTP 协议层异常（iter_content 阶段常见）
    http.client.HTTPException,  # HTTP 协议通用异常
    http.client.IncompleteRead,  # 读取不完整（网络中断）
    http.client.BadStatusLine,  # 状态行错误
    http.client.ResponseNotReady,  # 响应未准备好
    http.client.RemoteDisconnected,  # 远程连接被关闭
)

_CAN_RETRY_KEYWORDS = (
    "eof occurred in violation of protocol",
    "connection reset",
    "connection refused",
    "timeout",
)


def is_exception_can_retry(error: Exception) -> bool:
    for current_exc in _walk_exception_chain(error, max_depth=12):
        for retry_class in _CAN_RETRY_EXCEPTIONS:
            if isinstance(current_exc, retry_class):
                return True

    error_msg = str(error).lower()
    for keyword in _CAN_RETRY_KEYWORDS:
        if keyword in error_msg:
            return True

    return False


def _walk_exception_chain(error: Exception, max_depth: int):
    current_exc = error
    depth = 0

    yield current_exc

    while hasattr(current_exc, "__cause__") and current_exc.__cause__ is not None and depth < max_depth:
        current_exc = current_exc.__cause__
        depth += 1
        yield current_exc


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
            except Exception as error:
                if is_exception_can_retry(error):
                    last_error = error
                else:
                    raise error

            if i < self._retry_times:
                sleep(self._retry_sleep)

        if last_error is not None:
            raise last_error

        if last_response is not None:
            last_response.raise_for_status()

        raise RuntimeError("request failed")
