from enum import Enum, auto
import getpass
from pathlib import Path
import socket
from ssl import SSLContext
from typing import Optional, Any, Union
import os

from .base_protocol import BasePGProtocol, TransactionStatus, ProtocolStatus
from .zoneinfo import ZoneInfo

HAS_AF_UNIX = hasattr(socket, "AF_UNIX")


class SSLMode(Enum):
    DISABLE = auto()
    ALLOW = auto()
    PREFER = auto()
    REQUIRE = auto()
    DEFAULT = auto()


def _from_env(value: Any, varname: str) -> Any:
    if value is not None:
        return value
    return os.environ.get(f"PG{varname}")


class BaseConnection:

    _protocol: BasePGProtocol = None

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
            ) -> None:
        port = _from_env(port, "PORT")
        if port is None:
            port = 5432
        else:
            port = int(port)

        host = _from_env(host, "HOST")
        if host is None:
            if HAS_AF_UNIX:
                for host in ['/var/run/postgresql', '/tmp']:
                    if os.path.exists(f"{host}/.s.PGSQL.{port}"):
                        break
                else:
                    host = 'localhost'
            else:
                host = 'localhost'

        user = _from_env(user, "USER")
        if user is None:
            user = getpass.getuser()

        database = _from_env(database, "DATABASE")
        if database is None:
            database = user

        password = _from_env(password, "PASSWORD")

        use_af_unix = HAS_AF_UNIX and host.startswith("/")

        if ssl_mode == SSLMode.DEFAULT:
            if use_af_unix:
                ssl_mode = SSLMode.ALLOW
            else:
                ssl_mode = SSLMode.PREFER

        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._database = database
        self._tz_name = tz_name
        self._local_addr = local_addr
        self._use_af_unix = use_af_unix

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    @property
    def path(self) -> str:
        if not self._use_af_unix:
            raise ValueError("Not a unix socket connection")
        return str(Path(self._host) / f".s.PGSQL.{self._port}")

    @property
    def transaction_status(self) -> TransactionStatus:
        if self._protocol is None:
            return TransactionStatus.UNKNOWN
        return self._protocol.transaction_status

    @property
    def status(self) -> ProtocolStatus:
        if self._protocol is None:
            return ProtocolStatus.CLOSED
        return self._protocol.status

    @property
    def tz_info(self) -> Optional[ZoneInfo]:
        if self._protocol is None:
            return None
        return self._protocol.tz_info
