""" Asynchronous version of Protocol """

from asyncio import (
    BufferedProtocol, Transport, shield, Future, get_running_loop,
    BaseTransport, BaseProtocol)
from ssl import SSLContext, PROTOCOL_TLS_CLIENT, VerifyMode
from typing import Optional, Any, Union, cast, List, Tuple

import scramp

from .base_protocol import (
    _BasePGProtocol, PyBasePGProtocol, TransactionStatus, _STATUS_CONNECTED,
    _STATUS_CLOSED, _STATUS_READY_FOR_QUERY, _STATUS_SSL_REQUESTED,
    _STATUS_EXECUTING)
from .common import (
    ResultSet, CachedQueryExpired, Format, StatementDoesNotExist)


# pylint: disable-next=too-many-instance-attributes
class _AsyncPGProtocol(_BasePGProtocol):
    """ Async specific functionality of PG protocol """

    _transport: Transport

    def __init__(self) -> None:
        super().__init__()
        self._read_fut: Optional[Future[Any]] = None
        self._write_fut: Optional[Future[None]] = None
        self._loop = get_running_loop()

    def connection_made(self, transport: BaseTransport) -> None:
        """ Callback for transport """
        self._transport = cast(Transport, transport)
        self._status = _STATUS_CONNECTED

    def connection_lost(self, exc: Optional[Exception]) -> None:
        """ Callback for transport """
        self._status = _STATUS_CLOSED
        if exc is not None:
            self._set_exception(exc)

    def pause_writing(self) -> None:
        """ Callback for transport """
        self._write_fut = self._loop.create_future()

    def resume_writing(self) -> None:
        """ Callback for transport """
        if self._write_fut is not None:
            self._write_fut.set_result(None)

    async def write(self, data: bytes) -> None:
        """ Send data to the server """
        if self._write_fut is not None:
            await shield(self._write_fut)
        self._transport.write(data)

    async def writelines(self, data: List[bytes]) -> None:
        """ Send multiple data chunks to the server """
        if self._write_fut is not None:
            await shield(self._write_fut)
        self._transport.writelines(data)

    async def start_tls(
            self,
            ssl: Optional[SSLContext],
            server_hostname: Optional[str],
            ssl_handshake_timeout: Optional[float] = None,
    ) -> bool:
        """ Starts a TLS session. """
        # first ask server permission to use SSL/TLS
        self._status = _STATUS_SSL_REQUESTED
        ssl_ok = await self.request_ssl()
        self._status = _STATUS_CONNECTED
        if ssl_ok:
            if not isinstance(ssl, SSLContext):
                ssl = SSLContext(PROTOCOL_TLS_CLIENT)
                ssl.check_hostname = False
                ssl.verify_mode = VerifyMode.CERT_NONE
            self._transport = cast(Transport, await self._loop.start_tls(
                self._transport, cast(BaseProtocol, self), ssl,
                server_hostname=server_hostname,
                ssl_handshake_timeout=ssl_handshake_timeout))
        self._ssl_in_use = ssl_ok
        return ssl_ok

    async def request_ssl(self) -> bool:
        """ Request server to accept TLS upgrade """
        # SSL response has a different format. Just one byte. Set up for
        # receiving this.
        self._setup_ssl_request()

        self._read_fut = self._loop.create_future()
        await self.write(b'\0\0\0\x08\x04\xd2\x16/')
        return cast(bool, await self._read_fut)

    def get_channel_binding(self) -> Optional[Tuple[str, bytes]]:
        """ Returns the channel binding for SASL authentication """

        ssl_sock = self._transport.get_extra_info('ssl_object')
        if ssl_sock is None:
            # No SSL in use so channel binding is not used either
            return None
        return scramp.make_channel_binding("tls-server-end-point", ssl_sock)

    async def startup(  # pylint: disable=too-many-arguments
            self,
            user: str,
            database: Optional[str],
            application_name: Optional[str],
            tz_name: Optional[str],
            password: Union[None, str, bytes],
            prepare_threshold: int,
            cache_size: int,
    ) -> None:
        """ Start up connection, including authentication """
        message = self._startup_message(
            user, database, application_name, tz_name, password,
            prepare_threshold, cache_size)
        while isinstance(message, bytes):
            self._read_fut = self._loop.create_future()
            await self.write(message)
            message = await self._read_fut

    def cancel(self, backend_key: Tuple[int, int]) -> None:
        """ Sends a Cancel Request message """
        self._transport.write(self.cancel_message(backend_key))
        self._close()

    async def _execute(
            self,
            sql: str,
            parameters: Tuple[Any, ...],
            result_format: Format,
            raw_result: bool,
    ) -> ResultSet:
        msg = self.execute_message(
            sql, parameters, result_format, raw_result)
        self._read_fut = self._loop.create_future()
        await self.writelines(msg)
        self._status = _STATUS_EXECUTING
        return ResultSet(await self._read_fut)

    async def execute(
            self,
            sql: str,
            parameters: Tuple[Any, ...],
            result_format: Format,
            raw_result: bool,
    ) -> ResultSet:
        """ Execute a query text and return the result """
        try:
            return await self._execute(
                sql, parameters, result_format, raw_result)
        except (CachedQueryExpired, StatementDoesNotExist):
            # Cached statement result types are changed or is deallocated
            if self.transaction_status == TransactionStatus.IDLE:
                # Not in a transaction, so retry is possible.
                return await self._execute(
                    sql, parameters, result_format, raw_result)
            raise

    async def close(self) -> None:
        """ Closes the connection """
        if self._status == _STATUS_READY_FOR_QUERY:
            await self.writelines([self.terminate_message()])
        self._close()

    def _close(self) -> None:
        self._transport.close()
        self._status = _STATUS_CLOSED

    def _set_exception(self, ex: BaseException) -> None:
        if self._read_fut and not self._read_fut.done():
            self._read_fut.set_exception(ex)

    def _set_result(self, result: Any) -> None:
        if self._read_fut and not self._read_fut.done():
            self._read_fut.set_result(result)


class PyAsyncPGProtocol(PyBasePGProtocol, _AsyncPGProtocol, BufferedProtocol):
    """ Pure Python async version of PG protocol """


try:
    from ._pagio import CBasePGProtocol

    class AsyncPGProtocol(CBasePGProtocol, _AsyncPGProtocol, BufferedProtocol):
        """ C accelerated async version of PG protocol """

except ImportError:
    # Fallback to Pure Python
    AsyncPGProtocol = PyAsyncPGProtocol  # type: ignore
