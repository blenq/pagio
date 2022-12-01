""" Network type conversion functions """

from codecs import decode
from ipaddress import (
    ip_interface, ip_network, IPv4Interface, IPv6Interface, IPv4Network,
    IPv6Network, IPv4Address, IPv6Address)
from struct import unpack_from
from typing import Union, Callable, Any, cast, Tuple

from .common import ProtocolError, check_length_equal, Format
from .const import INETOID, CIDROID


def txt_inet_to_python(buf: memoryview) -> Union[IPv4Interface, IPv6Interface]:
    """ Converts text to IP interface """
    return ip_interface(decode(buf))


def txt_cidr_to_python(buf: memoryview) -> Union[IPv4Network, IPv6Network]:
    """ Converts text to IP network """
    return ip_network(decode(buf))


PGSQL_AF_INET = 2
PGSQL_AF_INET6 = PGSQL_AF_INET + 1


def get_read_ip_bin(cidr: int) -> Callable[[memoryview], Any]:
    """ Converts PG binary value to IP object """

    cons = cast(Callable[[Any], Any], [ip_interface, ip_network][cidr])

    def read_ip_bin(buf: memoryview) -> Any:
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

    return read_ip_bin


bin_inet_to_python = get_read_ip_bin(0)

bin_cidr_to_python = get_read_ip_bin(1)


def inet_to_pg(
        val: Union[IPv4Address, IPv6Address, IPv4Network, IPv6Network],
        oid: int,
) -> Tuple[int, str, bytes, int, Format]:
    val_bytes = str(val).encode()
    val_len = len(val_bytes)
    return oid, f"{val_len}s", val_bytes, val_len, Format.TEXT


def ip_interface_to_pg(
        val: Union[IPv4Address, IPv6Address]
) -> Tuple[int, str, bytes, int, Format]:
    return inet_to_pg(val, INETOID)


def ip_network_to_pg(
        val: Union[IPv4Network, IPv6Network]
) -> Tuple[int, str, bytes, int, Format]:
    return inet_to_pg(val, CIDROID)
