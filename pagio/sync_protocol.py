""" Synchronous version of Protocol """

import socket
from typing import Optional, Union, Any, List, Tuple

from .base_protocol import (
    _BasePGProtocol, Format, PyBasePGProtocol, _STATUS_READY_FOR_QUERY,
    TransactionStatus, _STATUS_CLOSED, _STATUS_CONNECTED)
from .common import ResultSet, CachedQueryExpired


NO_RESULT = object()


class _PGProtocol(_BasePGProtocol):
    """ Synchronous specific functionality of PG protocol """

    _prepare_threshold: int
    _cache_size: int

    def __init__(self, sock: socket.socket):
        super().__init__()
        self._sock = sock
        self._sync_result = NO_RESULT
        self._status = _STATUS_CONNECTED

    def startup(
            self,
            user: str,
            database: Optional[str],
            application_name: Optional[str],
            password: Union[None, str, bytes],
            *,
            tz_name: Optional[str],
            prepare_threshold: int,
            cache_size: int,
    ) -> None:
        """ Start up connection, including authentication """

        message = self._startup_message(
            user, database, application_name, tz_name, password)
        self._prepare_threshold = prepare_threshold
        self._cache_size = cache_size
        while isinstance(message, bytes):
            self.write(message)
            message = self.read()

    def write(self, data: bytes) -> None:
        """ Send data to the server """
        self._sock.sendall(data)

    def writelines(self, data: List[bytes]) -> None:
        for chunk in data:
            self.write(chunk)

    def read(self) -> Any:
        """ Read data from server and handle returned data """
        recv_into = self._sock.recv_into
        get_buffer = self.get_buffer
        buffer_updated = self.buffer_updated
        while self._sync_result is NO_RESULT:
            num_bytes = recv_into(get_buffer(-1))
            if num_bytes == 0:
                self._close()
                continue
            try:
                buffer_updated(num_bytes)
            except BaseException as ex:
                self._close()
                self._set_exception(ex)
        ret = self._sync_result
        self._sync_result = NO_RESULT
        if isinstance(ret, BaseException):
            raise ret
        return ret

    def _execute(
            self,
            sql: str,
            parameters: Tuple[Any, ...],
            result_format: Format,
    ) -> ResultSet:
        """ Execute a query text and return the result """
        self.writelines(
            self.execute_message(sql, parameters, result_format=result_format))
        return ResultSet(self.read())

    def execute(
            self,
            sql: str,
            parameters: Tuple[Any, ...],
            result_format: Format,
    ) -> ResultSet:
        """ Execute a query text and return the result """
        try:
            return self._execute(sql, parameters, result_format)
        except CachedQueryExpired:
            if self.transaction_status == TransactionStatus.IDLE:
                return self._execute(sql, parameters, result_format)
            raise

    def close(self) -> None:
        """ Closes the connection """
        if self.status == _STATUS_READY_FOR_QUERY:
            try:
                self.write(self.terminate_message())
            except:
                pass
        self._close()

    def _close(self) -> None:
        self._sock.close()
        self._status = _STATUS_CLOSED

    def _set_exception(self, ex: BaseException) -> None:
        self._sync_result = ex

    def _set_result(self, result: Any) -> None:
        self._sync_result = result


class PyPGProtocol(PyBasePGProtocol, _PGProtocol):
    """ Pure Python synchronous version of PG protocol """


try:
    from ._pagio import CBasePGProtocol

    class PGProtocol(CBasePGProtocol, _PGProtocol):
        """ C accelerated synchronous version of PG protocol """

except ImportError:
    # fallback to pure python
    PGProtocol = PyPGProtocol  # type: ignore
