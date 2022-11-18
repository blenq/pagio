""" Asynchronous version of Protocol """

from asyncio import (
    BufferedProtocol, Transport, shield, Future, get_running_loop,
    BaseTransport, BaseProtocol, wait, FIRST_COMPLETED, create_task,
    AbstractEventLoop, Queue
)
from codecs import decode
from inspect import isawaitable
from ssl import SSLContext, PROTOCOL_TLS_CLIENT, VerifyMode
from struct import Struct
from typing import Optional, Any, Union, cast, List, Tuple, Callable, Awaitable, Coroutine

import scramp

from .base_protocol import (
    _BasePGProtocol, PyBasePGProtocol, TransactionStatus, _STATUS_CONNECTED,
    _STATUS_CLOSED, _STATUS_READY_FOR_QUERY, _STATUS_SSL_REQUESTED,
    _STATUS_EXECUTING)
from .common import (
    ResultSet, CachedQueryExpired, Format, StatementDoesNotExist, CopyFile,
    Notification,
)


int_struct_pack = Struct('!i').pack


def async_wrap(
        loop: AbstractEventLoop, func: Callable[..., Any],
        ) -> Callable[..., Coroutine[Any, Any, None]]:

    """ Wraps a synchronous callable in a coroutine """

    async def _func(*args: Tuple[Any, ...]) -> Any:
        """ Coroutine to execute synchronous callable """
        return await loop.run_in_executor(None, func, *args)

    return _func


# pylint: disable-next=too-many-instance-attributes
class _AsyncPGProtocol(_BasePGProtocol):
    """ Async specific functionality of PG protocol """

    _transport: Transport

    def __init__(self) -> None:
        super().__init__()
        self._read_fut: Optional[Future[Any]] = None
        self._write_fut: Optional[Future[None]] = None
        self._loop = get_running_loop()
        self.notify_queue = Queue()

    def enqueue_notification(self, notification: Notification) -> None:
        self.notify_queue.put_nowait(notification)

    def handle_copy_in_response(self,  msg_buf: memoryview) -> None:
        """ Schedule the task to send copy messages """

        # Sending copy messages is asynchronous. Because this callback function
        # is not synchronous, use a Task
        if self._read_fut is not None:
            create_task(self.copy_in_response_task(msg_buf, self._read_fut))

    async def _copy_in_run_and_check(
            self,
            coro: Coroutine[Any, Any, Any],
            read_fut: Awaitable[Any],
    ) -> Tuple[bool, Any]:
        # The server can respond with an error during streaming content.
        # In that case, the final result Future might be set and control will
        # be transferred back to the caller, who might execute a new statement.
        # To prevent confusing the server, this
        # routine must stop sending messages in such a case. So check the final
        # result Future after running an asynchronous operation.
        # When the extended query protocol is used, the Future will not be set,
        # but still an error might have occurred. Even though sending further
        # CopyData message will not harm anything, let's just stop.
        # If the Simple Query protocol is used, PostgreSQL will automatically
        # send a ReadyForQuery as well, which will set the final result Future.
        # If the Extended version is used, a Sync message must be sent to the
        # server first.

        # wait simultaneously for operation and result Future
        coro_task = create_task(coro)
        done = await wait(
            (read_fut, coro_task), return_when=FIRST_COMPLETED)

        if read_fut in done[0]:
            # PostgreSQL has returned a ReadyForQuery message and probably
            # an Error message before that, but it is ready anyway. Done.
            return False, None

        if self._ex:
            # PostgreSQL has returned an Error
            if self._extended_query:
                # Extended query needs Sync message to receive Ready For Query
                await self.write(b'S\x00\x00\x00\x04')
            # ReadyForQuery not received yet, but should arrive shortly
            return False, None

        # Coroutine finished and no error received from server
        return True, coro_task.result()

    async def _copy_in_response_task(self, read_fut: Awaitable[Any]) -> None:
        """ Stream a local file object to the server. """

        if self.file_obj is None:
            raise Exception("I can't")

        # If read method of file object is not awaitable, assume it is blocking
        # run it using a threadpool. This is actually not true for a
        # BytesIO object for example, but better safe than sorry.
        read_method = getattr(self.file_obj, "read")
        if read_method is None:
            raise ValueError("Invalid input file, missing read method.")

        if not isawaitable(read_method):
            read_method = async_wrap(self._loop, read_method)

        while True:
            success, data = await self._copy_in_run_and_check(
                read_method(8192), read_fut)
            if not success:
                break
            if isinstance(data, str):
                data = data.encode()
            elif not isinstance(data, bytes):
                raise Exception("No bytes")
            if not data:
                # End of file, tell server
                if self._extended_query:
                    # CopyDone + Sync message
                    msg = b'c\x00\x00\x00\x04S\x00\x00\x00\x04'
                else:
                    # CopyDone message
                    msg = b'c\x00\x00\x00\x04'
                write_task = create_task(self.write(msg))
                await wait((read_fut, write_task), return_when=FIRST_COMPLETED)
                break
            # CopyData message
            message = [b'd', int_struct_pack(len(data) + 4), data]
            success, _ = await self._copy_in_run_and_check(
                self.writelines(message), read_fut)
            if not success:
                break

    async def copy_in_response_task(
            self,
            msg_buf: memoryview,  # pylint: disable=unused-argument
            read_fut: Awaitable[Any]) -> None:
        """ Asycnio Task coroutine to actually handle copy in response """
        try:
            await self._copy_in_response_task(read_fut)
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
            await self.write(msg)

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

    # pylint: disable-next=too-many-arguments
    async def _execute(
            self,
            sql: str,
            parameters: Tuple[Any, ...],
            result_format: Format,
            raw_result: bool,
            file_obj: Optional[CopyFile],
    ) -> ResultSet:
        msg = self.execute_message(
            sql, parameters, result_format, raw_result, file_obj)
        self._read_fut = self._loop.create_future()
        await self.writelines(msg)
        self._status = _STATUS_EXECUTING
        return ResultSet(await self._read_fut)

    # pylint: disable-next=too-many-arguments
    async def execute(
            self,
            sql: str,
            parameters: Tuple[Any, ...],
            result_format: Format,
            raw_result: bool,
            file_obj: Optional[CopyFile],
    ) -> ResultSet:
        """ Execute a query text and return the result """
        try:
            return await self._execute(
                sql, parameters, result_format, raw_result, file_obj)
        except (CachedQueryExpired, StatementDoesNotExist):
            # Cached statement result types are changed or is deallocated
            if self.transaction_status == TransactionStatus.IDLE:
                # Not in a transaction, so retry is possible.
                return await self._execute(
                    sql, parameters, result_format, raw_result, file_obj)
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
