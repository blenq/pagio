""" Synchronous connection class """

import socket
from ssl import SSLContext
from types import TracebackType
from typing import Optional, Any, Type, Tuple

from .base_protocol import Format
from .base_connection import BaseConnection, SSLMode
from .common import ResultSet
from .sync_protocol import PGProtocol


HAS_TCP_NODELAY = hasattr(socket, 'TCP_NODELAY')


# _protocol_class = PGProtocol


class Connection(BaseConnection):
    """ Synchronous connection class """

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
        self._protocol: PGProtocol = self._connect()

    def _connect(self) -> PGProtocol:
        if self._use_af_unix:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
            sock.connect(self.path)
        else:
            sock = socket.create_connection((self.host, self.port))
            if HAS_TCP_NODELAY:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        prot = PGProtocol(sock)
        prot.startup(
            self._user, self._database, "pagio", self._password,
            tz_name=self._tz_name, prepare_threshold=self._prepare_threshold,
            cache_size=self._cache_size,
        )
        return prot

    def execute(
            self,
            sql: str,
            *parameters: Tuple[Any, ...],
            result_format: Format = Format.TEXT,
    ) -> ResultSet:
        """ Execute a query text and return the result """
        return self._protocol.execute(
            sql, parameters, result_format=result_format)

    def close(self) -> None:
        """ Closes the connection """
        return self._protocol.close()

    def __enter__(self) -> 'Connection':
        return self

    def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc: Optional[BaseException],
            traceback: Optional[TracebackType],
    ) -> None:
        self.close()
