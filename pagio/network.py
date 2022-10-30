from codecs import decode
from ipaddress import ip_interface, ip_network
from struct import unpack_from

from .common import ProtocolError, check_length_equal


def txt_inet_to_python(buf: memoryview):
    return ip_interface(decode(buf))


def txt_cidr_to_python(buf: memoryview):
    return ip_network(decode(buf))


PGSQL_AF_INET = 2
PGSQL_AF_INET6 = PGSQL_AF_INET + 1


def get_read_ip_bin(cidr):

    cons = [ip_interface, ip_network][cidr]

    def read_ip_bin(buf: memoryview):
        family, mask, is_cidr, size = unpack_from("4B", buf)

        if is_cidr != cidr:
            raise ProtocolError("Wrong value for cidr flag")

        if family == PGSQL_AF_INET:
            check_length_equal(8, buf)
            addr_data = unpack_from("!I", buf, 4)[0]
        elif family == PGSQL_AF_INET6:
            check_length_equal(20, buf)
            addr_data = bytes(buf[4:])
        else:
            raise ProtocolError("Invalid address family")
        return cons((addr_data, mask))

    return read_ip_bin


bin_inet_to_python = get_read_ip_bin(0)

bin_cidr_to_python = get_read_ip_bin(1)
