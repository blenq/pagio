""" Synchronous connection class """

import socket
from ssl import SSLContext
from types import TracebackType
from typing import Optional, Any, Type, Tuple, Mapping

from .base_connection import BaseConnection, SSLMode
from .common import ResultSet, ServerError, Format, SyncCopyFile
from .sync_protocol import PGProtocol, NotificationQueue


HAS_TCP_NODELAY = hasattr(socket, 'TCP_NODELAY')


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
            local_addr: Optional[Tuple[str, int]] = None,
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
        self._protocol: Optional[PGProtocol] = self._connect(self._ssl_mode)
        self._notifications = NotificationQueue(self._protocol)

    def _connect(self, ssl_mode: SSLMode) -> PGProtocol:
        # connect socket
        if self._use_af_unix:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
            sock.connect(self.path)
        else:
            sock = socket.create_connection(
                (self.host, self.port), source_address=self._local_addr)
            if HAS_TCP_NODELAY:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        # Create protocol
        # note: pylint is wrong here because it doesn't read _pagio.pyi file
        prot = PGProtocol(sock)  # pylint: disable=abstract-class-instantiated

        if ssl_mode is SSLMode.PREFER or ssl_mode is SSLMode.REQUIRE:
            # Try to enable SSL
            ssl_ok = prot.start_tls(self._ssl, self._server_hostname)
            if not ssl_ok and ssl_mode is SSLMode.REQUIRE:
                raise Exception("Server refuses TLS upgrade")

        try:
            # login
            prot.startup(
                self._user, self._database, "pagio", self._password,
                self._tz_name, self._prepare_threshold, self._options,
                self._cache_size)
        except ServerError as ex:
            if ex.code == '28000' and ssl_mode == SSLMode.ALLOW:
                # Exception might be caused by SSL being required. Retry with
                # SSL.
                return self._connect(ssl_mode=SSLMode.REQUIRE)
            raise
        return prot

    @property
    def notifications(self) -> NotificationQueue:
        """ Notification Queue """
        if self._notifications is None:
            raise ValueError("Notification queue not set")
        return self._notifications

    def execute(
            self,
            sql: str,
            *parameters: Any,
            result_format: Format = Format.DEFAULT,
            raw_result: bool = False,
            file_obj: Optional[SyncCopyFile] = None,
    ) -> ResultSet:
        """ Execute a query text and return the result """
        if self._protocol is None:
            raise ValueError("Connection is closed.")
        return self._protocol.execute(
            sql, parameters, result_format, raw_result, file_obj)

    def close(self) -> None:
        """ Closes the connection """
        if self._protocol is None:
            return
        self._protocol.close()
        self._protocol = None

    def __enter__(self) -> 'Connection':
        return self

    def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc: Optional[BaseException],
            traceback: Optional[TracebackType],
    ) -> None:
        self.close()
