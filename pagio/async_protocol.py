from asyncio import (
    BufferedProtocol, Transport, shield, Future, get_running_loop, BaseTransport)
from typing import Optional, Any, Union, cast, List, Tuple

from .base_protocol import (
    _BasePGProtocol,
    # BasePGProtocol,
    PyBasePGProtocol, Format,
    TransactionStatus, _STATUS_CONNECTED, _STATUS_CLOSED,
    _STATUS_READY_FOR_QUERY)
from .common import ResultSet, CachedQueryExpired


class _AsyncPGProtocol(_BasePGProtocol):
    """ Async specific functionality of PG protocol """

    _transport: Transport

    def __init__(self) -> None:
        super().__init__()
        self._read_fut: Optional[Future[Any]] = None
        self._write_fut: Optional[Future[None]] = None
        self._loop = get_running_loop()

    def connection_made(self, transport: BaseTransport) -> None:
        self._transport = cast(Transport, transport)
        self._status = _STATUS_CONNECTED

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self._status = _STATUS_CLOSED
        if exc is not None:
            self._set_exception(exc)

    def pause_writing(self) -> None:
        self._write_fut = self._loop.create_future()

    def resume_writing(self) -> None:
        if self._write_fut is not None:
            self._write_fut.set_result(None)

    async def write(self, data: bytes) -> None:
        if self._write_fut is not None:
            await shield(self._write_fut)
        self._transport.write(data)

    async def writelines(self, data: List[bytes]) -> None:
        if self._write_fut is not None:
            await shield(self._write_fut)
        self._transport.writelines(data)

    async def startup(
            self,
            user: str,
            database: Optional[str],
            application_name: Optional[str],
            tz_name: Optional[str],
            password: Union[None, str, bytes],
            prepare_threshold: int,
            cache_size: int,
    ) -> None:
        message = self._startup_message(
            user, database, application_name, tz_name, password)
        self._prepare_threshold = prepare_threshold
        self._cache_size = cache_size

        while isinstance(message, bytes):
            self._read_fut = self._loop.create_future()
            await self.write(message)
            message = await self._read_fut

    async def _execute(
            self,
            sql: str,
            parameters: Tuple[Any, ...],
            result_format: Format,
    ) -> ResultSet:
        msg = self.execute_message(
            sql, parameters, result_format=result_format)
        self._read_fut = self._loop.create_future()
        await self.writelines(msg)
        return ResultSet(await self._read_fut)

    async def execute(
            self,
            sql: str,
            parameters: Tuple[Any, ...],
            result_format: Format,
    ) -> ResultSet:
        try:
            return await self._execute(sql, parameters, result_format)
        except CachedQueryExpired:
            # Cached statement is expired due to result types change.
            if self.transaction_status == TransactionStatus.IDLE:
                # Not in a transaction, so retry is possible
                return await self._execute(sql, parameters, result_format)
            raise

    async def close(self) -> None:
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
