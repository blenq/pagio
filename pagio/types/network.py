""" Network type conversion functions """

from codecs import decode
from ipaddress import (
    ip_interface, ip_network, IPv4Interface, IPv6Interface, IPv4Network,
    IPv6Network, IPv4Address, IPv6Address)
from struct import unpack_from
from typing import Union, Callable, Any, cast, Tuple

from .array import PGArray
from ..common import ProtocolError, check_length_equal, Format
from ..const import INETOID, CIDROID, INETARRAYOID
from .text import str_to_pg


def txt_inet_to_python(
        conn, buf: memoryview) -> Union[IPv4Interface, IPv6Interface]:
    """ Converts text to IP interface """
    return ip_interface(decode(buf))


def txt_cidr_to_python(
        conn, buf: memoryview) -> Union[IPv4Network, IPv6Network]:
    """ Converts text to IP network """
    return ip_network(decode(buf))


PGSQL_AF_INET = 2
PGSQL_AF_INET6 = 3


def bin_ip_to_python(buf: memoryview, cidr, cons) -> Any:
    family, mask, is_cidr, size = unpack_from("4B", buf)

    if is_cidr != cidr:
        raise ProtocolError("Wrong value for cidr flag")

    if family == PGSQL_AF_INET:
        if size != 4:
            raise ProtocolError("Invalid IPv4 value.")
        check_length_equal(8, buf)
        [addr_data] = unpack_from("!I", buf, 4)
    elif family == PGSQL_AF_INET6:
        if size != 16:
            raise ProtocolError("Invalid IPv6 value.")
        check_length_equal(20, buf)
        addr_data = bytes(buf[4:])
    else:
        raise ProtocolError("Invalid address family")
    return cons((addr_data, mask))


def bin_inet_to_python(conn, buf: memoryview) -> Any:
    return bin_ip_to_python(buf, 0, ip_interface)


def bin_cidr_to_python(conn, buf: memoryview) -> Any:
    return bin_ip_to_python(buf, 1, ip_network)


Inet = Union[IPv4Address, IPv6Address, IPv4Interface, IPv6Interface]


def ip_interface_to_pg(val: Inet) -> Tuple[int, str, bytes, int, Format]:
    """ Converts an IP address or interface to PG inet """
    return str_to_pg(str(val), INETOID)


def ip_network_to_pg(
        val: Union[IPv4Network, IPv6Network]
) -> Tuple[int, str, bytes, int, Format]:
    """ Converts an IP network to PG cidr """
    return str_to_pg(str(val), CIDROID)


class PGInet:  # pylint: disable=too-few-public-methods
    """ Class to facilitate JSON PG parameter """

    oid = INETOID

    def __init__(self, val: Union[str, Inet]) -> None:
        if not isinstance(
                val, (IPv4Address, IPv6Address, IPv4Interface, IPv6Interface)):
            val = ip_interface(val)
        self._val = val

    def __str__(self) -> str:
        return str(self._val)

    def __repr__(self):
        return repr(self._val)


class PGInetArray(PGArray):
    oid = INETARRAYOID
