""" Synchronous version of Protocol """

import socket
import sys
from typing import Optional, Union, Any, cast, Sequence

from .base_protocol import (
    BasePGProtocol, ProtocolStatus, Format, PyBasePGProtocol)
from .common import ResultSet


NO_RESULT = object()


class _PGProtocol:
    """ Synchronous Protocol """

    _prepare_threshold: int
    _cache_size: int

    def __init__(self, sock: socket.socket):
        super().__init__()
        self._sock = sock
        self._sync_result = NO_RESULT
        self._status = ProtocolStatus.CONNECTED

    def startup(
            self,
            user: str,
            database: Optional[str],
            application_name: Optional[str],
            password: Union[None, str, bytes],
            *,
            tz_name: Optional[str],
            prepare_threshold,
            cache_size,
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
        mem = memoryview(data)
        total_sent = 0
        data_len = len(data)
        while total_sent < data_len:
            sent = self._sock.send(mem[total_sent:])
            if sent == 0:
                raise RuntimeError("socket connection broken")
            total_sent += sent

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

    def execute(
            self,
            sql: str,
            parameters: Optional[Sequence[Any]],
            result_format: Format,
    ) -> ResultSet:
        """ Execute a query text and return the result """
        # msg = self.execute_message(
        #     sql, parameters, result_format=result_format)
        self.write(self.execute_message(
            sql, parameters, result_format=result_format))
        return cast(ResultSet, self.read())

    def close(self) -> None:
        """ Closes the connection """
        if self._status == ProtocolStatus.READY_FOR_QUERY:
            try:
                self.write(self.terminate_message())
            except:
                pass
        self._close()

    def _close(self) -> None:
        self._sock.close()
        self._status = ProtocolStatus.CLOSED

    def _set_exception(self, ex: BaseException) -> None:
        self._sync_result = ex

    def _set_result(self) -> None:
        self._sync_result = self._result


class PGProtocol(_PGProtocol, BasePGProtocol):
    pass


class PyPGProtocol(_PGProtocol, PyBasePGProtocol):
    pass
