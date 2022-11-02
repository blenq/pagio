from datetime import date
from decimal import Decimal
from ipaddress import IPv4Interface, IPv6Interface, IPv4Network, IPv6Network
import unittest
from uuid import UUID

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

    def test_bytea_result(self):
        self._cn.execute("SET bytea_output TO 'hex'")
        self._test_val_result("SELECT '\\x09686f695c'::bytea", b'\thoi\\')
        self._cn.execute("SET bytea_output TO 'escape'")
        self._test_val_result("SELECT '\\x09686f695c'::bytea", b'\thoi\\')

    def test_uuid_result(self):
        self._test_val_result(
            "SELECT '42d36a04-8ff1-4337-870e-51de61b19771'::uuid",
            UUID('42d36a04-8ff1-4337-870e-51de61b19771'))

    def test_date_result(self):
        self._test_val_result(
            "SELECT '2021-03-15'::date", date(2021, 3, 15))
        self._test_val_result(
            "SELECT '0900-03-15'::date", date(900, 3, 15))
        self._test_val_result(
            "SELECT '20210-03-15'::date", '20210-03-15')
        self._test_val_result(
            "SELECT '2021-03-15 BC'::date", '2021-03-15 BC')
        self._test_val_result(
            "SELECT 'infinity'::date", 'infinity')
        self._test_val_result(
            "SELECT '-infinity'::date", '-infinity')
        self._cn.execute("SET DateStyle TO postgres, dmy")
        res = self._cn.execute(
            "SELECT '2021-03-15'::date", result_format=Format.TEXT)
        self.assertEqual("15-03-2021", res[0][0])
        res = self._cn.execute(
            "SELECT '2021-03-15'::date", result_format=Format.BINARY)
        self.assertEqual(date(2021, 3, 15), res[0][0])


class PyConnTypeCase(ConnTypeCase):
    @classmethod
    def setUpClass(cls) -> None:
        sync_connection.PGProtocol = sync_protocol.PyPGProtocol

    @classmethod
    def tearDownClass(cls) -> None:
        sync_connection.PGProtocol = sync_protocol.PGProtocol