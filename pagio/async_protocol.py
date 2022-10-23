from asyncio import (
    BufferedProtocol, Transport, shield, Future, get_running_loop)
from typing import Optional, Any, Union, cast, Sequence, List

from .base_protocol import (
    BasePGProtocol, PyBasePGProtocol, ProtocolStatus, Format,
    TransactionStatus)
from .common import ResultSet, CachedQueryExpired


class _AsyncPGProtocol:

    _transport: Transport

    def __init__(self) -> None:
        super().__init__()
        self._read_fut: Optional[Future[Any]] = None
        self._write_fut: Optional[Future[None]] = None
        self._loop = get_running_loop()

    def connection_made(  # type: ignore[override]
            self, transport: Transport) -> None:
        self._transport = transport
        self._status = ProtocolStatus.CONNECTED

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self._status = ProtocolStatus.CLOSED
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
            parameters: Sequence[Any],
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
            parameters: Sequence[Any],
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
        if self._status == ProtocolStatus.READY_FOR_QUERY:
            await self.write(self.terminate_message())
        self._close()

    def _close(self) -> None:
        self._transport.close()
        self._status = ProtocolStatus.CLOSED

    def _set_exception(self, ex: BaseException) -> None:
        if self._read_fut and not self._read_fut.done():
            self._read_fut.set_exception(ex)

    def _set_result(self, result) -> None:
        if self._read_fut and not self._read_fut.done():
            self._read_fut.set_result(result)


class AsyncPGProtocol(_AsyncPGProtocol, BasePGProtocol, BufferedProtocol):
    pass


class PyAsyncPGProtocol(_AsyncPGProtocol, PyBasePGProtocol, BufferedProtocol):
    pass