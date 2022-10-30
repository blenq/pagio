import decimal
from decimal import Decimal
from ipaddress import IPv4Interface, IPv6Interface, IPv4Network, IPv6Network
import unittest

from pagio import Connection, sync_connection, sync_protocol, Format


class ConnTypeCase(unittest.TestCase):

    def setUp(self) -> None:
        self._cn = Connection(database="postgres")

    def tearDown(self) -> None:
        self._cn.close()

    def _test_val_result(self, sql, val):
        res = self._cn.execute(sql)
        self.assertEqual(val, res[0][0])
        res = self._cn.execute(sql, result_format=Format.TEXT)
        self.assertEqual(val, res[0][0])
        res = self._cn.execute(sql, result_format=Format.BINARY)
        self.assertEqual(val, res[0][0])

    def test_ipv4_inet_result(self):
        self._test_val_result(
            "SELECT '192.168.0.1'::inet", IPv4Interface("192.168.0.1"))
        self._test_val_result(
            "SELECT '192.168.0.1/24'::inet", IPv4Interface("192.168.0.1/24"))

    def test_ipv6_inet_result(self):
        self._test_val_result(
            "SELECT '2001:db8:85a3:0:0:8a2e:370:7334'::inet",
            IPv6Interface("2001:db8:85a3:0:0:8a2e:370:7334"))
        self._test_val_result(
            "SELECT '2001:db8:85a3:0:0:8a2e:370:7334/64'::inet",
            IPv6Interface("2001:db8:85a3:0:0:8a2e:370:7334/64"))

    def test_ipv4_cidr_result(self):
        self._test_val_result(
            "SELECT '192.168.0.0/24'::cidr", IPv4Network("192.168.0.0/24"))

    def test_ipv6_cidr_result(self):
        self._test_val_result(
            "SELECT '2001:db8:85a3:0:0:8a2e:0:0/96'::cidr",
            IPv6Network("2001:db8:85a3:0:0:8a2e:0:0/96"))

    def _assert_decimal_equals(self, expected, actual):
        if expected.is_nan() and actual.is_nan():
            return
        self.assertEqual(expected, actual)
        self.assertGreaterEqual(
            len(actual.as_tuple()[1]), len(expected.as_tuple()[1]))

    def _test_numeric_val(self, sql, val):
        res = self._cn.execute(sql)
        self._assert_decimal_equals(val, res[0][0])
        res = self._cn.execute(sql, result_format=Format.TEXT)
        self._assert_decimal_equals(val, res[0][0])
        res = self._cn.execute(sql, result_format=Format.BINARY)
        self._assert_decimal_equals(val, res[0][0])

    def test_numeric_result(self):
        self._test_numeric_val(
            "SELECT '123.456'::numeric(12, 5)", Decimal("123.456"))
        self._test_numeric_val("SELECT 'NaN'::numeric", Decimal("NaN"))
        self._test_numeric_val(
            "SELECT '123456789012345678901234567890'::numeric",
            Decimal("123456789012345678901234567890"))
        self._test_numeric_val(
            "SELECT '0.000000000000001230'::numeric",
            Decimal("0.000000000000001230"))
        self._test_numeric_val(
            "SELECT '-123456789012345678901234567890'::numeric",
            Decimal("-123456789012345678901234567890"))
        self._test_numeric_val(
            "SELECT '-0.000000000000001230'::numeric",
            Decimal("-0.000000000000001230"))
        self._test_numeric_val(
            "SELECT '9990E+99'::numeric", Decimal("9990E+99"))
        self._test_numeric_val(
            "SELECT '9990E-98'::numeric", Decimal("9990E-98"))
        self._test_numeric_val("SELECT '0'::numeric", Decimal("0"))
        self._test_numeric_val(
            "SELECT '0.000000'::numeric", Decimal("0.000000"))
        self._test_numeric_val(
            "SELECT '1234567890.0987654321'::numeric",
            Decimal("1234567890.0987654321"))


class PyConnTypeCase(ConnTypeCase):
    @classmethod
    def setUpClass(cls) -> None:
        sync_connection.PGProtocol = sync_protocol.PyPGProtocol

    @classmethod
    def tearDownClass(cls) -> None:
        sync_connection.PGProtocol = sync_protocol.PGProtocol