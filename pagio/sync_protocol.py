""" Synchronous version of Protocol """

import socket
from struct import Struct
from ssl import SSLContext, PROTOCOL_TLS_CLIENT, VerifyMode, SSLSocket
from typing import Optional, Union, Any, List, Tuple

import scramp

from .base_protocol import (
    _BasePGProtocol, PyBasePGProtocol, _STATUS_READY_FOR_QUERY,
    TransactionStatus, _STATUS_CLOSED, _STATUS_CONNECTED,
    _STATUS_SSL_REQUESTED, _STATUS_EXECUTING)
from .common import (
    ResultSet, CachedQueryExpired, Format, StatementDoesNotExist, SyncCopyFile)


NO_RESULT = object()

int_struct_pack = Struct('!i').pack


class _PGProtocol(_BasePGProtocol):
    """ Synchronous specific functionality of PG protocol """

    _prepare_threshold: int
    _cache_size: int
    file_obj: Optional[SyncCopyFile]

    def __init__(self, sock: socket.socket):
        super().__init__()
        self._sock = sock
        self._sync_result = NO_RESULT
        self._status = _STATUS_CONNECTED

    def _handle_copy_in_response(self) -> None:
        if self.file_obj is None:
            raise Exception("I can't")

        read_method = getattr(self.file_obj, "read")
        if read_method is None:
            raise ValueError("Invalid input file, missing read method.")
        while True:
            data = read_method(4096)
            if isinstance(data, str):
                data = data.encode()
            elif not isinstance(data, bytes):
                raise Exception("No bytes")
            if not data:
                if self._extended_query:
                    # CopyDone + Sync message
                    msg = b'c\x00\x00\x00\x04S\x00\x00\x00\x04'
                else:
                    # CopyDone message
                    msg = b'c\x00\x00\x00\x04'
                self.write(msg)
                break
            message = [b'd', int_struct_pack(len(data) + 4), data]
            self.writelines(message)

    # pylint: disable-next=unused-argument
    def handle_copy_in_response(self, msg_buf: memoryview) -> None:
        """ Handles a copy in response """
        try:
            self._handle_copy_in_response()
        except Exception as ex:  # pylint: disable=broad-except
            # Something went wrong when reading the file. Store exception and
            # notify server.
            self._ex = ex
            # Send copy fail
            if self._extended_query:
                # CopyFail and Sync
                msg = b'f\0\0\0\x05\0S\x00\x00\x00\x04'
            else:
                msg = b'f\0\0\0\x05\0'  # CopyFail message
            self.write(msg)

    def start_tls(
            self,
            ssl: Optional[SSLContext],
            server_hostname: Optional[str],
            ssl_handshake_timeout: Optional[float] = None,
    ) -> bool:
        """ Starts a TLS session. """

        # first ask server permission to use SSL/TLS
        self._status = _STATUS_SSL_REQUESTED
        ssl_ok = self.request_ssl()
        self._status = _STATUS_CONNECTED
        if not ssl_ok:
            return False
        if not isinstance(ssl, SSLContext):
            ssl = SSLContext(PROTOCOL_TLS_CLIENT)
            ssl.check_hostname = False
            ssl.verify_mode = VerifyMode.CERT_NONE
        self._sock.settimeout(ssl_handshake_timeout)
        self._sock = ssl.wrap_socket(
            self._sock, server_hostname=server_hostname)
        self._sock.settimeout(None)

        return True

    def request_ssl(self) -> bool:
        """ Request server to accept TLS upgrade """
        # SSL response has a different format. Just one byte. Set up for
        # receiving this.
        self._setup_ssl_request()
        self.write(b'\0\0\0\x08\x04\xd2\x16/')
        return bool(self.read())

    def get_channel_binding(self) -> Optional[Tuple[str, bytes]]:
        """ Returns the channel binding for SASL authentication """

        if isinstance(self._sock, SSLSocket):
            return scramp.make_channel_binding(
                "tls-server-end-point", self._sock)
        # No SSL in use so channel binding is not used either
        return None

    def startup(  # pylint: disable=too-many-arguments
            self,
            user: str,
            database: Optional[str],
            application_name: Optional[str],
            password: Union[None, str, bytes],
            tz_name: Optional[str],
            prepare_threshold: int,
            cache_size: int,
    ) -> None:
        """ Start up connection, including authentication """

        message = self._startup_message(
            user, database, application_name, tz_name, password,
            prepare_threshold, cache_size)
        while isinstance(message, bytes):
            self.write(message)
            message = self.read()

    def write(self, data: bytes) -> None:
        """ Send data to the server """
        try:
            self._sock.sendall(data)
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException:
            self._close()
            raise

    def writelines(self, data: List[bytes]) -> None:
        """ Send multiple data chunks to the server """
        self.write(b''.join(data))

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
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as ex:  # pylint: disable=broad-except
                self._close()
                self._set_exception(ex)
        ret = self._sync_result
        self._sync_result = NO_RESULT
        if isinstance(ret, BaseException):
            raise ret
        return ret

    # pylint: disable-next=too-many-arguments
    def _execute(
            self,
            sql: str,
            parameters: Tuple[Any, ...],
            result_format: Format,
            raw_result: bool,
            file_obj: Optional[SyncCopyFile],
    ) -> ResultSet:
        """ Execute a query text and return the result """
        self.writelines(
            self.execute_message(
                sql, parameters, result_format, raw_result, file_obj))
        self._status = _STATUS_EXECUTING
        return ResultSet(self.read())

    # pylint: disable-next=too-many-arguments
    def execute(
            self,
            sql: str,
            parameters: Tuple[Any, ...],
            result_format: Format,
            raw_result: bool,
            file_obj: Optional[SyncCopyFile],
    ) -> ResultSet:
        """ Execute a query text and return the result """
        try:
            return self._execute(
                sql, parameters, result_format, raw_result, file_obj)
        except (CachedQueryExpired, StatementDoesNotExist):
            if self.transaction_status == TransactionStatus.IDLE:
                return self._execute(
                    sql, parameters, result_format, raw_result, file_obj)
            raise

    def close(self) -> None:
        """ Closes the connection """
        if self.status == _STATUS_READY_FOR_QUERY:
            # noinspection PyBroadException
            try:
                self.write(self.terminate_message())
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException:  # pylint: disable=broad-except
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
