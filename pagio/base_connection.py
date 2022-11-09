""" Base connection functionality """

from enum import Enum, auto
import getpass
from pathlib import Path
import socket
from ssl import SSLContext
from typing import Optional, Dict, Tuple, Union
import os

from .base_protocol import _BasePGProtocol, TransactionStatus, ProtocolStatus
from .zoneinfo import ZoneInfo

HAS_AF_UNIX = hasattr(socket, "AF_UNIX")


class SSLMode(Enum):
    """ SSL mode of connection """
    DISABLE = auto()
    ALLOW = auto()
    PREFER = auto()
    REQUIRE = auto()
    DEFAULT = auto()


def _get_port(port: Optional[int]) -> int:
    if port is not None:
        return port
    port_str = os.environ.get("PGPORT")
    if port_str is None:
        return 5432
    return int(port_str)


def _get_host(host: Optional[str], port: int) -> str:
    if host is not None:
        return host
    host = os.environ.get("PGHOST")
    if host is not None:
        return host
    if HAS_AF_UNIX:
        for dir_name in ['/var/run/postgresql', '/tmp']:
            if os.path.exists(f"{dir_name}/.s.PGSQL.{port}"):
                return dir_name
    return 'localhost'


def _get_user(user: Optional[str]) -> str:
    if user is not None:
        return user
    user = os.environ.get("PGUSER")
    if user is not None:
        return user
    return getpass.getuser()


def _get_database(database: Optional[str], user: str) -> str:
    if database is not None:
        return database
    database = os.environ.get("PGDATABASE")
    if database is not None:
        return database
    return user


def _get_pwd(
        password: Optional[Union[bytes, str]]) -> Optional[Union[bytes, str]]:
    if password is not None:
        return password
    return os.environ.get("PGPASSWORD")


class BaseConnection:  # pylint: disable=too-many-instance-attributes
    """ Base Connection """

    _protocol: Optional[_BasePGProtocol] = None

    def __init__(  # pylint: disable=too-many-arguments
            self,
            host: Optional[str] = None,
            port: Optional[int] = None,
            database: Optional[str] = None,
            user: Optional[str] = None,
            password: Optional[Union[str, bytes]] = None,
            tz_name: Optional[str] = None,
            *,
            ssl_mode: SSLMode = SSLMode.DEFAULT,
            ssl: Optional[SSLContext] = None,
            local_addr: Optional[Tuple[str, int]] = None,
            server_hostname: Optional[str] = None,
            prepare_threshold: int = 5,
            cache_size: int = 100,
            ) -> None:

        port = _get_port(port)
        host = _get_host(host, port)
        user = _get_user(user)
        database = _get_database(database, user)
        password = _get_pwd(password)

        use_af_unix = HAS_AF_UNIX and host.startswith("/")

        if ssl_mode == SSLMode.DEFAULT:
            if use_af_unix:
                ssl_mode = SSLMode.ALLOW
            else:
                ssl_mode = SSLMode.PREFER
        if server_hostname is None and not use_af_unix:
            server_hostname = host

        self._ssl_mode = ssl_mode
        self._ssl = ssl
        self._server_hostname = server_hostname
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._database = database
        self._tz_name = tz_name
        self._local_addr = local_addr
        self._use_af_unix = use_af_unix
        prepare_threshold = int(prepare_threshold)
        if prepare_threshold < 0:
            raise ValueError("Negative value for prepare_threshold.")
        cache_size = int(cache_size)
        if cache_size < 1:
            raise ValueError("Invalid value for cache_size")
        self._prepare_threshold = prepare_threshold
        self._cache_size = cache_size
        self._ssl_in_use = False

    @property
    def host(self) -> str:
        """ Server host """
        return self._host

    @property
    def port(self) -> int:
        """ Server port """
        return self._port

    @property
    def path(self) -> str:
        """ Unix socket path """
        if not self._use_af_unix:
            raise ValueError("Not a unix socket connection")
        return str(Path(self._host) / f".s.PGSQL.{self._port}")

    @property
    def transaction_status(self) -> TransactionStatus:
        """ Current transaction status """
        if self._protocol is None:
            return TransactionStatus.UNKNOWN
        return self._protocol.transaction_status

    @property
    def status(self) -> ProtocolStatus:
        """ Current protocol status """
        if self._protocol is None:
            return ProtocolStatus.CLOSED
        return self._protocol.status

    @property
    def tz_info(self) -> Optional[ZoneInfo]:
        """ Session timezone """
        if self._protocol is None:
            return None
        return self._protocol.tz_info

    @property
    def server_parameters(self) -> Dict[str, str]:
        """ Dictionary of server parameters """
        if self._protocol is None:
            return {}
        return self._protocol.server_parameters
