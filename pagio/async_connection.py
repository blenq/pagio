""" Asynchronous connection class """

import asyncio
from ssl import SSLContext
from types import TracebackType
from typing import Optional, Any, Generator, Type, Tuple

from .async_protocol import AsyncPGProtocol
from .base_connection import BaseConnection, SSLMode
from .base_protocol import ProtocolStatus
from .common import ResultSet, ServerError, Format


class AsyncConnection(BaseConnection):
    """ Asynchronous connection class """

    _protocol: AsyncPGProtocol

    # pylint: disable-next=too-many-arguments
    def __init__(
            self,
            host: Optional[str] = None,
            port: Optional[int] = None,
            database: Optional[str] = None,
            user: Optional[str] = None,
            password: Optional[str] = None,
            *,
            tz_name: Optional[str] = None,
            ssl_mode: SSLMode = SSLMode.DEFAULT,
            ssl: Optional[SSLContext] = None,
            local_addr: Any = None,
            server_hostname: Optional[str] = None,
            prepare_threshold: int = 5,
            cache_size: int = 100,
    ) -> None:
        super().__init__(
            host, port, database, user, password, tz_name, ssl_mode=ssl_mode,
            ssl=ssl, local_addr=local_addr, server_hostname=server_hostname,
            prepare_threshold=prepare_threshold, cache_size=cache_size,
        )

    def __await__(self) -> Generator[Any, None, 'AsyncConnection']:
        return self._connect(self._ssl_mode).__await__()

    async def _connect_protocol(self, ssl_mode: SSLMode) -> AsyncPGProtocol:
        loop = asyncio.get_running_loop()
        if self._use_af_unix:
            conn = await loop.create_unix_connection(
                AsyncPGProtocol, self.path)
        else:
            conn = await loop.create_connection(
                AsyncPGProtocol, self.host, self.port,
                local_addr=self._local_addr)
        protocol = conn[1]
        if ssl_mode is SSLMode.PREFER or ssl_mode is SSLMode.REQUIRE:
            ssl_ok = await protocol.start_tls(
                self._ssl, self._server_hostname)
            if not ssl_ok and ssl_mode is SSLMode.REQUIRE:
                raise Exception("Server refuses TLS upgrade")
        return protocol

    async def _connect(self, ssl_mode: SSLMode) -> 'AsyncConnection':
        if self._protocol is not None:
            raise ValueError("Connection has been awaited already")
        self._protocol = await self._connect_protocol(ssl_mode)
        try:
            await self._protocol.startup(
                self._user, self._database, "pagio", self._tz_name,
                self._password, prepare_threshold=self._prepare_threshold,
                cache_size=self._cache_size)
        except ServerError as ex:
            if ex.code == '28000' and ssl_mode == SSLMode.ALLOW:
                # Exception might be caused by SSL being required. Retry with
                # SSL.
                self._protocol = None
                return await self._connect(ssl_mode=SSLMode.REQUIRE)
            raise
        return self

    async def cancel(self) -> None:
        """ Cancels an executing statement. """

        if self.status is not ProtocolStatus.EXECUTING:
            # shortcut
            return

        # First open a new connection, using the same connection parameters
        if self._protocol.ssl_in_use:
            ssl_mode = SSLMode.REQUIRE
        else:
            ssl_mode = SSLMode.DISABLE
        prot = await self._connect_protocol(ssl_mode)

        # If existing connection is still executing, send Cancel Request on the
        # new connection with backend key info of the current connection
        if self.status is ProtocolStatus.EXECUTING:
            prot.cancel(self._protocol.backend_key)
        await prot.close()

    async def execute(
            self,
            sql: str,
            *parameters: Tuple[Any, ...],
            result_format: Format = Format.TEXT,
            raw_result: bool = False,
    ) -> ResultSet:
        """ Execute a query text and return the result """
        # return await self._protocol.execute(
        #     sql, parameters, result_format, raw_result)
        try:
            task = asyncio.create_task(self._protocol.execute(
                sql, parameters, result_format, raw_result))
            return await asyncio.shield(task)
        except asyncio.CancelledError as ex:
            # Task is cancelled, for example due to a timeout. Try to cancel
            # statement server side as well to keep connection usable.
            try:
                await asyncio.wait_for(self.cancel(), 2)
                # Task was shielded, so we can wait for it again.
                await asyncio.wait_for(task, 2)
            finally:
                raise ex

    async def close(self) -> None:
        """ Closes the connection """
        await self._protocol.close()

    async def __aenter__(self) -> 'AsyncConnection':
        return self

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc: Optional[BaseException],
            traceback: Optional[TracebackType],
    ) -> None:
        await self.close()
