""" Asynchronous version of Protocol """
from asyncio import (
    BufferedProtocol, Transport, Task, Future, get_running_loop,
    BaseTransport, BaseProtocol, wait, FIRST_COMPLETED, create_task,
    AbstractEventLoop, Queue, CancelledError, Event,
)
from collections import deque
from inspect import isawaitable
from io import TextIOBase
from ssl import SSLContext, PROTOCOL_TLS_CLIENT, VerifyMode
from typing import (
    Optional, Any, Union, cast, List, Tuple, Callable, Awaitable, Coroutine,
    Deque, Mapping, Set,
)

import scramp

from .base_protocol import (
    _BasePGProtocol, PyBasePGProtocol, TransactionStatus, _STATUS_CONNECTED,
    _STATUS_CLOSED, _STATUS_READY_FOR_QUERY, _STATUS_SSL_REQUESTED,
    _STATUS_EXECUTING, _STATUS_CLOSING)
from .common import (
    ResultSet, CachedQueryExpired, Format, StatementDoesNotExist, CopyFile,
    Notification, InterfaceError, ServerError, Severity, int4_to_bytes,
)


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
        self._loop = get_running_loop()
        self._read_fut: Optional[Future[Any]] = None
        self._write_event = Event()
        self._close_fut: Optional[Future[None]] = None
        self._copy_out_fut: Optional[Future[Any]] = None
        self.notify_queue: Queue[Notification] = Queue()
        self._copy_out_data: Optional[Deque[Optional[bytes]]] = None
        self._tasks: Set['Task[Any]'] = set()

    def _create_task(self, coro: Coroutine[Any, None, Any]) -> 'Task[Any]':
        task = create_task(coro)
        task.add_done_callback(self._tasks.discard)
        return task

    def enqueue_notification(self, notification: Notification) -> None:
        self.notify_queue.put_nowait(notification)

    def handle_copy_data_response(self, msg_buf: memoryview) -> None:
        if self._copy_out_data is None:
            if self._copy_out_fut and not self._copy_out_fut.done():
                raise ValueError("Unexpected COPY OUT data")
            return
        if self._copy_out_fut is None:
            return
        self._copy_out_data.append(bytes(msg_buf))
        if not self._copy_out_fut.done():
            self._copy_out_fut.set_result(None)

    def handle_copy_done_response(self, msg_buf: memoryview) -> None:
        if self._copy_out_data is None:
            if self._copy_out_fut and not self._copy_out_fut.done():
                self._copy_out_fut.cancel()
                raise ValueError("Unexpected COPY OUT data")
            return
        if self._copy_out_fut is None:
            return
        self._copy_out_data.append(None)
        if not self._copy_out_fut.done():
            self._copy_out_fut.set_result(None)

    async def copy_out_task(self, user_fut: 'Future[Any]') -> None:
        try:
            write_method = self.file_obj.write  # type: ignore
        except AttributeError as ex:
            raise ValueError(
                "Invalid output file, missing write method.") from ex
        if not isawaitable(write_method):
            write_method = async_wrap(self._loop, write_method)
        is_text_file = (
                isinstance(self.file_obj, TextIOBase) or
                "b" not in getattr(self.file_obj, "mode", "b"))
        if self._copy_out_fut is None:
            return
        if not self._copy_out_fut.done():
            self._transport.resume_reading()
        while True:
            await self._copy_out_fut
            self._transport.pause_reading()
            while self._copy_out_data:
                data = self._copy_out_data.popleft()
                if data is None:
                    self._transport.resume_reading()
                    self._copy_out_data = None
                    await self._read_fut  # type: ignore
                    try:
                        result = self._read_fut.result()  # type: ignore
                    except Exception as ex:
                        if not user_fut.done():
                            user_fut.set_exception(ex)
                    else:
                        if not user_fut.done():
                            user_fut.set_result(result)
                    return
                if is_text_file:
                    data = data.decode()  # type: ignore
                await write_method(data)  # type: ignore
            self._transport.resume_reading()
            self._copy_out_fut = self._loop.create_future()

    def handle_copy_out_response(self,  msg_buf: memoryview) -> None:
        if self.file_obj is None:
            raise ValueError("File object is not set")
        self._transport.pause_reading()
        self._copy_out_data = deque()
        self._copy_out_fut = self._loop.create_future()

        # Because writing to the file is async, it can happen that the result
        # future is set before the task is finished writing.
        # Remove the future, the caller is waiting on and create
        # a new one instead. The old one will be set with the result of the new
        # one by the write task when it is finished.
        user_fut = self._read_fut
        if not user_fut.done():  # type: ignore
            self._read_fut = self._loop.create_future()
        self._create_task(self.copy_out_task(user_fut))  # type: ignore

    def handle_copy_in_response(self,  msg_buf: memoryview) -> None:
        """ Schedule the task to send copy messages """

        # Sending copy messages is asynchronous. Because this callback function
        # is not synchronous, use a Task
        if self._read_fut is not None:
            self._create_task(self.copy_in_task(msg_buf, self._read_fut))

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
        coro_task = self._create_task(coro)
        done = await wait(
            (read_fut, coro_task), return_when=FIRST_COMPLETED)

        if read_fut in done[0]:
            # PostgreSQL has returned a ReadyForQuery message and probably
            # an Error message before that, but it is ready anyway. Done.
            if coro_task not in done[0]:
                coro_task.cancel()
            return False, None

        if self._ex:
            # PostgreSQL has returned an Error
            if coro_task not in done[0]:
                coro_task.cancel()
            if self._extended_query:
                # Extended query needs Sync message to receive Ready For Query
                await self.write(b'S\x00\x00\x00\x04')
            # ReadyForQuery not received yet, but should arrive shortly
            return False, None

        # Coroutine finished and no error received from server
        return True, coro_task.result()

    async def _copy_in_task(self, read_fut: Awaitable[Any]) -> None:
        """ Stream a local file object to the server. """

        if self.file_obj is None:
            raise Exception("I can't")

        # If read method of file object is not awaitable, assume it is blocking
        # and run it using a threadpool. This is actually not true for a
        # BytesIO object for example, but better safe than sorry.
        read_method = getattr(self.file_obj, "read", None)
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
                write_task = self._create_task(self.write(msg))
                await wait((read_fut, write_task), return_when=FIRST_COMPLETED)
                break
            # CopyData message
            message = [b'd', int4_to_bytes(len(data) + 4), data]
            success, _ = await self._copy_in_run_and_check(
                self.writelines(message), read_fut)
            if not success:
                break

    async def copy_in_task(
            self,
            msg_buf: memoryview,  # pylint: disable=unused-argument
            read_fut: Awaitable[Any]) -> None:
        """ Asycnio Task coroutine to actually handle copy in response """
        try:
            await self._copy_in_task(read_fut)
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
        self._write_event.set()
        self._status = _STATUS_CONNECTED

    def connection_lost(self, exc: Optional[Exception]) -> None:
        """ Callback for transport """
        self._status = _STATUS_CLOSED
        if exc is not None:
            self._set_exception(exc)
            if self._close_fut is not None and not self._close_fut.done():
                self._close_fut.set_exception(exc)
        else:
            if self._read_fut is not None and not self._read_fut.done():
                if (self._ex and isinstance(self._ex, ServerError) and
                        self._ex.severity == Severity.FATAL):
                    ex: Exception = self._ex
                else:
                    ex = InterfaceError("Connection is closed.")
                self._read_fut.set_exception(ex)
            if self._close_fut is not None and not self._close_fut.done():
                self._close_fut.set_result(None)

    def pause_writing(self) -> None:
        """ Callback for transport """
        self._write_event.clear()

    def resume_writing(self) -> None:
        """ Callback for transport """
        self._write_event.set()

    async def write(self, data: bytes) -> None:
        """ Send data to the server """
        await self._write_event.wait()
        self._transport.write(data)

    async def writelines(self, data: List[bytes]) -> None:
        """ Send multiple data chunks to the server """
        await self._write_event.wait()
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
        self._transport.write(b'\0\0\0\x08\x04\xd2\x16/')
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
            options: Optional[Mapping[str, Optional[str]]],
            cache_size: int,
    ) -> None:
        """ Start up connection, including authentication """
        message = self._startup_message(
            user, database, application_name, tz_name, password,
            options, prepare_threshold, cache_size)
        while isinstance(message, bytes):
            self._read_fut = self._loop.create_future()
            self._transport.write(message)
            message = await self._read_fut

    async def cancel(self, backend_key: Tuple[int, int]) -> None:
        """ Sends a Cancel Request message """
        self._transport.write(self.cancel_message(backend_key))
        await self._close()

    def __await__(self) -> Any:
        """ Wait until ReadyForQuery is sent """
        if self._read_fut is None:
            raise ValueError("Can not await Protocol")
        return self._read_fut.__await__()

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
        self._transport.writelines(msg)
        self._status = _STATUS_EXECUTING
        return ResultSet(await self._read_fut)

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
        except CancelledError:
            if self._status == _STATUS_EXECUTING:
                # Execution is cancelled, for example by timeout. To keep
                # protocol in usable state, the AsyncConnection will try to
                # cancel it server side as well. Recreate the future, so
                # the AsyncConnection can wait for cancellation to finish
                self._read_fut = self._loop.create_future()
            raise

    async def close(self) -> None:
        """ Closes the connection """
        if self._status == _STATUS_READY_FOR_QUERY:
            self._transport.write(self.terminate_message())
        await self._close()

    async def _close(self) -> None:
        if not self._transport.is_closing():
            if self._close_fut is None:
                self._close_fut = self._loop.create_future()
                self._transport.close()
                self._status = _STATUS_CLOSING
        if self._close_fut is not None:
            await self._close_fut

    def _set_exception(self, ex: BaseException) -> None:
        if isinstance(ex, ServerError) and ex.severity == Severity.FATAL:
            self._ex = ex
            self._create_task(self._close())
        elif self._read_fut and not self._read_fut.done():
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
