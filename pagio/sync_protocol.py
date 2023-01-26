""" Synchronous version of Protocol """

from collections import deque
from io import TextIOBase
import socket
from ssl import SSLContext, PROTOCOL_TLS_CLIENT, VerifyMode, SSLSocket
from typing import Optional, Union, Any, List, Tuple, Deque, Mapping

import scramp

from .base_protocol import (
    _BasePGProtocol, PyBasePGProtocol, _STATUS_READY_FOR_QUERY,
    TransactionStatus, _STATUS_CLOSED, _STATUS_CONNECTED,
    _STATUS_SSL_REQUESTED, _STATUS_EXECUTING)
from .common import (
    ResultSet, CachedQueryExpired, Format, StatementDoesNotExist, SyncCopyFile,
    Notification, InterfaceError, ServerError, Severity, int4_to_bytes,
)


NO_RESULT = object()


class _PGProtocol(_BasePGProtocol):
    """ Synchronous specific functionality of PG protocol """

    _prepare_threshold: int
    _cache_size: int
    file_obj: Optional[SyncCopyFile]

    def __init__(self, sock: socket.socket):
        super().__init__()
        self.sock: Optional[socket.socket] = sock
        self._sync_result = NO_RESULT
        self._status = _STATUS_CONNECTED
        self.notify_queue: Deque[Notification] = deque()

    def enqueue_notification(self, notification: Notification) -> None:
        self.notify_queue.append(notification)

    def handle_copy_out_response(self, msg_buf: memoryview) -> None:
        """ Handle a copy out response """
        if self.file_obj is None:
            raise ValueError("File object is not set")
        try:
            write_method = self.file_obj.write  # type: ignore
        except AttributeError as ex:
            raise ValueError(
                "Invalid output file, missing write method.") from ex
        is_text_file = (
                isinstance(self.file_obj, TextIOBase) or
                "b" not in getattr(self.file_obj, "mode", "b"))
        if is_text_file:
            self._write_method = lambda data: write_method(data.decode())
        else:
            self._write_method = write_method

    def handle_copy_data_response(self, msg_buf: memoryview) -> None:
        """ Handle a copy data response """
        self._write_method(bytes(msg_buf))  # type: ignore

    def handle_copy_done_response(self, msg_buf: memoryview) -> None:
        """ Handle a copy done response """

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
            self.writelines([b'd', int4_to_bytes(len(data) + 4), data])

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

        if self.sock is None:
            raise InterfaceError("Connection is closed.")

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
        self.sock.settimeout(ssl_handshake_timeout)
        self.sock = ssl.wrap_socket(
            self.sock, server_hostname=server_hostname)
        self.sock.settimeout(None)

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

        if isinstance(self.sock, SSLSocket):
            return scramp.make_channel_binding(
                "tls-server-end-point", self.sock)
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
            options: Optional[Mapping[str, Optional[str]]],
            cache_size: int,
    ) -> None:
        """ Start up connection, including authentication """

        message = self._startup_message(
            user, database, application_name, tz_name, password,
            options, prepare_threshold, cache_size)
        while isinstance(message, bytes):
            self.write(message)
            message = self.read()

    def write(self, data: bytes) -> None:
        """ Send data to the server """
        try:
            self.sock.sendall(data)  # type: ignore
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException:
            self._close()
            raise

    def writelines(self, data: List[bytes]) -> None:
        """ Send multiple data chunks to the server """
        self.write(b''.join(data))

    def poll(self) -> None:
        """ Make a single read pass """
        num_bytes = self.sock.recv_into(self.get_buffer(-1))  # type: ignore
        if num_bytes:
            self.buffer_updated(num_bytes)
        else:
            self._close()
            raise ValueError("Connection closed")

    def read(self) -> Any:
        """ Read data from server and handle returned data """
        while self._sync_result is NO_RESULT:
            try:
                self.poll()
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as ex:  # pylint: disable=broad-except
                self._close()
                self._set_exception(ex)
        ret = self._sync_result
        self._sync_result = NO_RESULT
        if isinstance(ret, BaseException):
            if isinstance(ret, ServerError) and ret.severity == Severity.FATAL:
                self._close()
            raise ret
        return ret

    def _execute(
            self,
            sql: str,
            parameters: Tuple[Any, ...],
            result_format: Format,
            raw_result: bool,
            file_obj: Optional[SyncCopyFile],
    ) -> ResultSet:
        """ Execute a query text and return the result """
        if self.sock is None:
            raise InterfaceError("Connection is closed.")
        self.writelines(
            self.execute_message(
                sql, parameters, result_format, raw_result, file_obj))
        self._status = _STATUS_EXECUTING
        return ResultSet(self.read())

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
                # Cached statement is changed or remove on server. No
                # transaction in progress, so it can be retried.
                return self._execute(
                    sql, parameters, result_format, raw_result, file_obj)
            raise

    def close(self) -> None:
        """ Closes the connection """
        if self.status == _STATUS_READY_FOR_QUERY:
            # noinspection PyBroadException
            msg = self.terminate_message()
            try:
                self.write(msg)
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException:  # pylint: disable=broad-except
                pass
        self._close()

    def _close(self) -> None:
        if self.sock is not None:
            self.sock.close()
            self._status = _STATUS_CLOSED
            self.sock = None

    def _set_exception(self, ex: BaseException) -> None:
        self._sync_result = ex

    def _set_result(self, result: Any) -> None:
        self._sync_result = result


class QueueEmpty(Exception):
    """ Raised when no notifications are available """


class NotificationQueue:
    """ Queue for notifications """
    def __init__(self, protocol: _PGProtocol) -> None:
        self._protocol = protocol

    def get(self, timeout: Optional[float] = None) -> Notification:
        """ Gets a notification """
        if self.empty():
            self._protocol.sock.settimeout(timeout)  # type: ignore
            while not self._protocol.notify_queue:
                try:
                    self._protocol.poll()
                except (socket.timeout, BlockingIOError) as ex:
                    raise QueueEmpty from ex
                finally:
                    self._protocol.sock.settimeout(None)  # type: ignore

        return self._protocol.notify_queue.popleft()

    def get_nowait(self) -> Notification:
        """ Gets a notification from the queue without waiting """
        return self.get(timeout=0)

    def qsize(self) -> int:
        """ Returns the size of the queue """
        return len(self._protocol.notify_queue)

    def empty(self) -> bool:
        """ Indicates if the queue is empty """
        return not bool(self._protocol.notify_queue)


class PyPGProtocol(PyBasePGProtocol, _PGProtocol):
    """ Pure Python synchronous version of PG protocol """


try:
    from ._pagio import CBasePGProtocol
except ImportError:
    # fallback to pure python
    PGProtocol = PyPGProtocol  # type: ignore
else:
    class PGProtocol(CBasePGProtocol, _PGProtocol):
        """ C accelerated synchronous version of PG protocol """
