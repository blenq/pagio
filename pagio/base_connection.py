""" Base connection functionality """

from datetime import tzinfo
from enum import Enum, auto
import getpass
from pathlib import Path
import re
import socket
from ssl import SSLContext
from typing import Any, Optional, Tuple, Union, Mapping
from types import MappingProxyType
import os

from .base_protocol import _BasePGProtocol, TransactionStatus, ProtocolStatus
from .types import ResConverter

HAS_AF_UNIX = hasattr(socket, "AF_UNIX")


class SSLMode(Enum):
    """ SSL mode of connection """
    DISABLE = auto()
    """ SSL is disabled. """
    ALLOW = auto()
    """ SSL connection is only tried, when it fails without. """
    PREFER = auto()
    """ SSL is tried first, but when it fails, connection proceeds without it.
    """
    REQUIRE = auto()
    """ SSL connection is required and connection fails without it. """
    DEFAULT = auto()
    """ :py:attr:`SSLMode.PREFER` for TCP connections,
        :py:attr:`SSLMode.ALLOW` for unix sockets. """


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
        for dir_name in ('/var/run/postgresql', '/tmp'):
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
    """ Base Connection class.

    Instantiating this class is not meaningful. It serves as the base for
    both :py:class:`Connection <pagio.Connection>` and
    :py:class:`AsyncConnection <pagio.AsyncConnection>`.

    :param str host: The host name of the server. If the host name is None
        the environment variable PGHOST will be examined. If it is still None
        it will first look for an existing Unix socket in /var/run/postgresql
        and /tmp if Unix sockets are available for the system. As a last
        fallback it will use "localhost".

    :param int port: The server port to connect to. If the port is None the
        value of the environment variable PGPORT is used. If it is still None
        it will use 5432 as the default port.

    :param str database: The database name to connect to. If it is None the
        value of the environment variable PGDATABASE is used. If that is also
        not set it will use the username as database name.

    :param user: The username for the session. If it is not provided, the
        value of the environment variable PGUSER is used. As last fallback the
        current OS username will be used.
        Edge case:
        A Pagio connection will use UTF-8 for all text to binary conversions.
        PostgreSQL uses arbitrary binary strings for usernames, depending on
        the actual encoding in use when the user was created. If the binary
        string can not be decoded to UTF-8, provide the username as a
        :external+py3:py:class:`bytes` string, to prevent conversions.

    :param password: The password for the session. If it is None, the value of
        the environment variable PGPASSWORD is used. The same edge case as
        described for the username applies to the password. If you encounter
        that problem, provide the password as a :external+py3:py:class:`bytes`
        string.

    :param tz_name: The timezone name to use for the session. Also see
        :ref:`Timezones`.

    :param ssl_mode: Indicates if SSL should be used and if it is required.

    :param ssl: :external+py3:py:class:`SSLContext <ssl.SSLContext>` to use.
        If it is None and the connection uses SSL it will use a default
        non-validating SSLContext.

    :param local_addr: If given, is a (local_host, local_port) tuple used to
        bind the TCP socket locally.

    :param server_hostname: The hostname that is used for SSL validation. If
        it is None, the original host argument is used instead.

    :param int prepare_threshold: The number of successful executions before a
        statement will be prepared with a name, which makes the prepared
        statement reusable for faster execution. Setting it to zero, will
        disable :ref:`statement caching <Statement caching>`.

    :param cache_size: The maximum size of the statement cache.

    """
    _protocol: Optional[_BasePGProtocol] = None

    def __init__(
            self,
            host: Optional[str] = None,
            port: Optional[int] = None,
            database: Optional[str] = None,
            user: Optional[str] = None,
            password: Optional[Union[str, bytes]] = None,
            *,
            tz_name: Optional[str] = None,
            ssl_mode: SSLMode = SSLMode.DEFAULT,
            ssl: Optional[SSLContext] = None,
            local_addr: Optional[Tuple[str, int]] = None,
            server_hostname: Optional[str] = None,
            prepare_threshold: int = 5,
            options: Optional[Mapping[str, Optional[str]]],
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
        self._options = options
        prepare_threshold = int(prepare_threshold)
        if prepare_threshold < 0:
            raise ValueError("Negative value for prepare_threshold.")
        cache_size = int(cache_size)
        if cache_size < 1:
            raise ValueError("Invalid value for cache_size")
        self._prepare_threshold = prepare_threshold
        self._cache_size = cache_size
        self._ssl_in_use = False
        self._server_parameters: Optional[Mapping[str, str]] = None

    @property
    def host(self) -> str:
        """ PostgreSQL server host name

        This is a path to a directory if Unix sockets are used, or else a
        network hostname.
        """
        return self._host

    @property
    def port(self) -> int:
        """ The PostgreSQL server port """
        return self._port

    @property
    def path(self) -> str:
        """ Unix socket path

        If a Unix socket is used, this returns the full path to the socket.
        """
        if not self._use_af_unix:
            raise ValueError("Unix sockets not in use.")
        return str(Path(self._host) / f".s.PGSQL.{self._port}")

    @property
    def transaction_status(self) -> TransactionStatus:
        """ Current transaction status of the connection. """
        if self._protocol is None:
            return TransactionStatus.UNKNOWN
        return self._protocol.transaction_status

    @property
    def status(self) -> ProtocolStatus:
        """ Current protocol status of the connection. """
        if self._protocol is None:
            return ProtocolStatus.CLOSED
        return self._protocol.status

    @property
    def tzinfo(self) -> Optional[tzinfo]:
        """ Session timezone """
        if self._protocol is None:
            return None
        return self._protocol.tzinfo

    @property
    def server_parameters(self) -> Mapping[str, str]:
        """ Dictionary like object of server parameters.

        These parameters are supplied by the server. The following keys will be
        present: "server_version", "server_encoding", "client_encoding",
        "application_name", "default_transaction_read_only", "in_hot_standby",
        "is_superuser", "session_authorization", "DateStyle", "IntervalStyle",
        "TimeZone", "integer_datetimes", "standard_conforming_strings".

        "default_transaction_read_only" and "in_hot_standby" will be present
        only for server versions starting from 14.

        """
        if self._protocol is None:
            return {}
        if self._server_parameters is None:
            self._server_parameters = MappingProxyType(
                self._protocol.server_parameters)
        return self._server_parameters

    @property
    def server_version(self) -> int:
        """ Returns the server version as an integer.

        Can be used for comparisons.

        """
        if self._protocol is None:
            raise ValueError("Connection not established")
        version_str = self._protocol.server_parameters["server_version"]
        match = re.match(r"(\d+)\.(\d+)", version_str)
        if match is None:
            raise ValueError("Can not parse server version string.")
        return int(match.group(1)) * 10000 + int(match.group(2))

    def register_res_converter(
            self, type_oid: int,
            txt_conv: ResConverter[Any],
            res_conv: ResConverter[Any],
            array_oid: int = 0,
            delim: str = ",",
    ) -> None:
        """ Register a converter for a PG type. """
        self._protocol.register_res_converter(  # type: ignore
            type_oid, txt_conv, res_conv, array_oid, delim)
