import asyncio
from ssl import SSLContext
from types import TracebackType
from typing import Optional, Any, Generator, Type, Tuple

from .async_protocol import AsyncPGProtocol
from .base_protocol import Format
from .base_connection import BaseConnection, SSLMode
from .common import ResultSet


class AsyncConnection(BaseConnection):

    _protocol: AsyncPGProtocol

    def __init__(
            self,
            host: Optional[str] = None,
            port: Optional[int] = None,
            database: Optional[str] = None,
            user: Optional[str] = None,
            password: Optional[str] = None,
            tz_name: Optional[str] = None,
            *,
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
        return self._connect().__await__()

    async def _connect(self) -> 'AsyncConnection':
        if self._protocol is not None:
            raise ValueError("Connection has been awaited already")
        loop = asyncio.get_running_loop()
        if self._use_af_unix:
            cn = await loop.create_unix_connection(
                AsyncPGProtocol, self.path)
        else:
            cn = await loop.create_connection(
                AsyncPGProtocol, self.host, self.port,
                local_addr=self._local_addr)
        self._protocol = cn[1]
        await self._protocol.startup(
            self._user, self._database, "pagio", self._tz_name,
            self._password, prepare_threshold=self._prepare_threshold,
            cache_size=self._cache_size)
        return self

    async def execute(
            self,
            sql: str,
            *parameters: Tuple[Any, ...],
            result_format: Format = Format.TEXT,
            raw_result: bool = False,
    ) -> ResultSet:
        return await self._protocol.execute(
            sql, parameters, result_format, raw_result)

    async def close(self) -> None:
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
