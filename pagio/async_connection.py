""" Asynchronous connection class """

import asyncio
from ssl import SSLContext
from types import TracebackType
from typing import Optional, Any, Generator, Type, Mapping

from .async_protocol import AsyncPGProtocol
from .base_connection import BaseConnection, SSLMode
from .base_protocol import ProtocolStatus
from .common import ResultSet, ServerError, Format, CopyFile, Notification


class AsyncConnection(BaseConnection):
    """ Asynchronous connection class

    After instantiating it must be awaited to actually connect to the
    PostgreSQL server. For example::

        import asyncio
        from pagio import AsyncConnection

        async def main():
            cn = await AsyncConnection(database="test")
            ...

        asyncio.run(main())


    """
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
            options: Optional[Mapping[str, Optional[str]]] = None,
            cache_size: int = 100,
    ) -> None:
        super().__init__(
            host, port, database, user, password, tz_name=tz_name,
            ssl_mode=ssl_mode, ssl=ssl, local_addr=local_addr,
            server_hostname=server_hostname,
            prepare_threshold=prepare_threshold, options=options,
            cache_size=cache_size,
        )
        self._protocol: Optional[AsyncPGProtocol] = None
        self._notify_queue: Optional['asyncio.Queue[Notification]'] = None

    def __await__(self) -> Generator[Any, None, 'AsyncConnection']:
        return self._connect(self._ssl_mode).__await__()

    @property
    def notifications(self) -> Optional['asyncio.Queue[Notification]']:
        """ Notification queue """
        return self._notify_queue

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
        self._notify_queue = self._protocol.notify_queue
        try:
            await self._protocol.startup(
                self._user, self._database, "pagio", self._tz_name,
                self._password, prepare_threshold=self._prepare_threshold,
                options=self._options, cache_size=self._cache_size)
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

        if self._protocol is None:
            raise ValueError("Connection is closed.")

        if self.status is not ProtocolStatus.EXECUTING:
            # shortcut
            return

        # First open a new connection, using the same connection parameters
        if self._protocol.ssl_in_use:
            ssl_mode = SSLMode.REQUIRE
        else:
            ssl_mode = SSLMode.DISABLE

        prot = await self._connect_protocol(ssl_mode)

        try:
            # If existing connection is still executing, send Cancel Request
            # on the new connection with backend key info of the current
            # connection
            if self.status is ProtocolStatus.EXECUTING:
                await prot.cancel(self._protocol.backend_key)
        finally:
            await prot.close()

    async def execute(
            self,
            sql: str,
            *parameters: Any,
            result_format: Format = Format.DEFAULT,
            raw_result: bool = False,
            file_obj: Optional[CopyFile] = None,
    ) -> ResultSet:
        """ Execute a query text and return the result """

        if self._protocol is None:
            raise ValueError("Connection is closed.")
        try:
            return await self._protocol.execute(
                sql, parameters, result_format, raw_result, file_obj)
        except asyncio.CancelledError as ex:
            # Task is cancelled, for example due to a timeout. Try to cancel
            # statement server side as well to keep connection usable.
            try:
                if self.status is not ProtocolStatus.READY_FOR_QUERY:
                    await asyncio.wait_for(self.cancel(), 2)
                # Cancel has been sent on 2nd connection, now wait for
                # first to actually notice.
                await asyncio.wait_for(self._protocol, 2)
            finally:
                if self.status is not ProtocolStatus.READY_FOR_QUERY:
                    await self.close()
                raise ex

    async def close(self) -> None:
        """ Closes the connection """
        if self._protocol is None:
            return
        await self._protocol.close()
        self._protocol = None

    async def __aenter__(self) -> 'AsyncConnection':
        return self

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc: Optional[BaseException],
            traceback: Optional[TracebackType],
            ) -> None:
        await self.close()
