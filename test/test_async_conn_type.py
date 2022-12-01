import uuid
from datetime import date, datetime, timedelta, timezone, time
from decimal import Decimal
from ipaddress import (
    IPv4Interface, IPv6Interface, IPv4Network, IPv6Network, IPv4Address,
    IPv6Address)
from uuid import UUID, uuid4
try:
    from unittest import IsolatedAsyncioTestCase
except ImportError:
    from later.unittest.backport.async_case import IsolatedAsyncioTestCase

from pagio import (
    AsyncConnection, async_connection, async_protocol, Format, PGJson)
from pagio.zoneinfo import ZoneInfo


class ConnTypeCase(IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self._cn = await AsyncConnection(database="postgres")

    async def asyncTearDown(self) -> None:
        self._cn.close()

    async def _test_val_result(self, sql, val, *params):
        res = await self._cn.execute(sql, *params)
        self.assertEqual(val, res[0][0])
        res = await self._cn.execute(sql, *params, result_format=Format.TEXT)
        self.assertEqual(val, res[0][0])
        res = await self._cn.execute(sql, *params, result_format=Format.BINARY)
        self.assertEqual(val, res[0][0])

    async def test_ipv4_inet_result(self):
        await self._test_val_result(
            "SELECT '192.168.0.1'::inet", IPv4Interface("192.168.0.1"))
        await self._test_val_result(
            "SELECT '192.168.0.1/24'::inet", IPv4Interface("192.168.0.1/24"))

    async def test_ipv4_inet_param(self):
        val = IPv4Interface("192.168.0.1")
        await self._test_val_result("SELECT $1", val, val)
        val = IPv4Interface("192.168.0.1/24")
        await self._test_val_result("SELECT $1", val, val)
        val = IPv4Address("192.168.0.10")
        await self._test_val_result(
            "SELECT $1", IPv4Interface("192.168.0.10/32"), val)

    async def test_ipv6_inet_result(self):
        await self._test_val_result(
            "SELECT '2001:db8:85a3:0:0:8a2e:370:7334'::inet",
            IPv6Interface("2001:db8:85a3:0:0:8a2e:370:7334"))
        await self._test_val_result(
            "SELECT '2001:db8:85a3:0:0:8a2e:370:7334/64'::inet",
            IPv6Interface("2001:db8:85a3:0:0:8a2e:370:7334/64"))

    async def test_ipv6_inet_param(self):
        val = IPv6Interface("2001:db8:85a3:0:0:8a2e:370:7334")
        await self._test_val_result("SELECT $1", val, val)
        val = IPv6Interface("2001:db8:85a3:0:0:8a2e:370:7334/64")
        await self._test_val_result("SELECT $1", val, val)
        val = IPv6Address("2001:db8:85a3:0:0:8a2e:370:7334")
        await self._test_val_result(
            "SELECT $1", IPv6Interface("2001:db8:85a3:0:0:8a2e:370:7334"), val)

    async def test_ipv4_cidr_result(self):
        await self._test_val_result(
            "SELECT '192.168.0.0/24'::cidr", IPv4Network("192.168.0.0/24"))

    async def test_ipv4_cidr_param(self):
        val = IPv4Network('192.168.0.0/24')
        await self._test_val_result("SELECT $1", val, val)

    async def test_ipv6_cidr_result(self):
        await self._test_val_result(
            "SELECT '2001:db8:85a3:0:0:8a2e:0:0/96'::cidr",
            IPv6Network("2001:db8:85a3:0:0:8a2e:0:0/96"))

    async def test_ipv6_cidr_param(self):
        val = IPv6Network('2001:db8:85a3:0:0:8a2e:0:0/96')
        await self._test_val_result("SELECT $1", val, val)

    def _assert_decimal_equals(self, expected, actual):
        if expected.is_nan() and actual.is_nan():
            return
        self.assertEqual(expected, actual)

    async def _test_numeric_val(self, sql, val):
        res = await self._cn.execute(sql)
        self._assert_decimal_equals(val, res[0][0])
        res = await self._cn.execute(sql, result_format=Format.TEXT)
        self._assert_decimal_equals(val, res[0][0])
        res = await self._cn.execute(sql, result_format=Format.BINARY)
        self._assert_decimal_equals(val, res[0][0])

    async def test_numeric_result(self):
        await self._test_numeric_val(
            "SELECT '123.456'::numeric", Decimal("123.456"))
        await self._test_numeric_val(
            "SELECT '123.456'::numeric(12, 5)", Decimal("123.456"))
        await self._test_numeric_val("SELECT 'NaN'::numeric", Decimal("NaN"))
        await self._test_numeric_val(
            "SELECT '123456789012345678901234567890'::numeric",
            Decimal("123456789012345678901234567890"))
        await self._test_numeric_val(
            "SELECT '0.000000000000001230'::numeric",
            Decimal("0.000000000000001230"))
        await self._test_numeric_val(
            "SELECT '-123456789012345678901234567890'::numeric",
            Decimal("-123456789012345678901234567890"))
        await self._test_numeric_val(
            "SELECT '-0.000000000000001230'::numeric",
            Decimal("-0.000000000000001230"))
        await self._test_numeric_val(
            "SELECT '9990E+99'::numeric", Decimal("9990E+99"))
        await self._test_numeric_val(
            "SELECT '9990E-98'::numeric", Decimal("9990E-98"))
        await self._test_numeric_val("SELECT '0'::numeric", Decimal("0"))
        await self._test_numeric_val(
            "SELECT '0.000000'::numeric", Decimal("0.000000"))
        await self._test_numeric_val(
            "SELECT '1234567890.0987654321'::numeric",
            Decimal("1234567890.0987654321"))

    async def test_bytea_result(self):
        await self._cn.execute("SET bytea_output TO 'hex'")
        await self._test_val_result("SELECT '\\x686f69'::bytea", b'hoi')
        await self._cn.execute("SET bytea_output TO 'escape'")
        await self._test_val_result("SELECT '\\x686f69'::bytea", b'hoi')

    async def test_bytea_param(self):
        val = b'\thoi\\'
        await self._test_val_result("SELECT $1 -- no-cache bytea", val, val)

    async def test_uuid_result(self):
        await self._test_val_result(
            "SELECT '42d36a04-8ff1-4337-870e-51de61b19771'::uuid",
            UUID('42d36a04-8ff1-4337-870e-51de61b19771'))

    async def test_uuid_param(self):
        val = uuid4()
        await self._test_val_result("SELECT $1", val, val)

    async def test_date_result(self):
        await self._test_val_result(
            "SELECT '2021-03-15'::date", date(2021, 3, 15))
        await self._test_val_result(
            "SELECT '0900-03-15'::date", date(900, 3, 15))
        await self._test_val_result(
            "SELECT '20210-03-15'::date", '20210-03-15')
        await self._test_val_result(
            "SELECT '2021-03-15 BC'::date", '2021-03-15 BC')
        await self._test_val_result(
            "SELECT 'infinity'::date", 'infinity')
        await self._test_val_result(
            "SELECT '-infinity'::date", '-infinity')
        await self._cn.execute("SET DateStyle TO postgres, dmy")
        res = await self._cn.execute(
            "SELECT '2021-03-15'::date", result_format=Format.TEXT)
        self.assertEqual("15-03-2021", res[0][0])
        res = await self._cn.execute(
            "SELECT '2021-03-15'::date", result_format=Format.BINARY)
        self.assertEqual(date(2021, 3, 15), res[0][0])

    async def test_date_param(self):
        val = date(2021, 3, 15)
        await self._test_val_result("SELECT $1", val, val)
        val = date(1980, 3, 15)
        await self._test_val_result("SELECT $1", val, val)
        val = date(1980, 1, 15)
        await self._test_val_result("SELECT $1", val, val)
        val = date(2000, 3, 15)
        await self._test_val_result("SELECT $1", val, val)
        val = date(1, 1, 1)
        await self._test_val_result("SELECT $1", val, val)
        val = date(9999, 12, 31)
        await self._test_val_result("SELECT $1", val, val)

    async def test_timestamptz_param(self):
        await self._cn.execute("SET TIMEZONE TO 'Europe/Amsterdam'")
        val = datetime(2021, 3, 15, 14, 10, 3)
        res = await self._cn.execute(
            "SELECT $1::timestamptz", val, result_format=Format.TEXT)
        tz2 = ZoneInfo("Europe/Amsterdam")
        val2 = datetime(2021, 3, 15, 14, 10, 3, tzinfo=tz2)
        db_val = res.rows[0][0]
        self.assertEqual(val2, db_val)
        self.assertEqual(tz2, db_val.tzinfo)

        res = await self._cn.execute(
            "SELECT $1::timestamptz", val, result_format=Format.BINARY)
        db_val = res.rows[0][0]
        self.assertEqual(val2, db_val)
        self.assertEqual(val2, db_val)
        self.assertEqual(ZoneInfo("Europe/Amsterdam"), db_val.tzinfo)

        res = await self._cn.execute(
            "SELECT $1 -- no-cache 1", val2, result_format=Format.TEXT)
        db_val = res.rows[0][0]
        self.assertEqual(val2, db_val)
        self.assertEqual(tz2, db_val.tzinfo)

        res = await self._cn.execute(
            "SELECT $1 -- no-cache 2", val2, result_format=Format.BINARY)
        db_val = res.rows[0][0]
        self.assertEqual(val2, db_val)
        self.assertEqual(ZoneInfo("Europe/Amsterdam"), db_val.tzinfo)

    async def test_time_result(self):
        await self._test_val_result("SELECT '13:12'::time", time(13, 12))
        await self._test_val_result(
            "SELECT '13:12:34.23'::time", time(13, 12, 34, 230000))
        await self._test_val_result("SELECT '24:00'::time", time(0))
        await self._test_val_result(
            "SELECT '13:12:34.239876'::time", time(13, 12, 34, 239876))

    async def test_time_param(self):
        val = time(13, 12)
        await self._test_val_result("SELECT $1 -- no-cache time", val, val)

    async def test_timetz_result(self):
        await self._test_val_result(
            "SELECT '13:12+01'::timetz",
            time(13, 12, tzinfo=timezone(timedelta(hours=1))))
        await self._test_val_result(
            "SELECT '13:12:22-02:30'::timetz",
            time(13, 12, 22, tzinfo=timezone(
                -1 * timedelta(hours=2, minutes=30))))
        await self._test_val_result(
            "SELECT '13:12:22+02:30:12'::timetz",
            time(13, 12, 22, tzinfo=timezone(
                timedelta(hours=2, minutes=30, seconds=12))))
        await self._test_val_result(
            "SELECT '24:00:00-02:30:12'::timetz",
            time(0, tzinfo=timezone(
                -1 * timedelta(hours=2, minutes=30, seconds=12))))

    async def test_timetz_param(self):
        val = time(13, 12, tzinfo=timezone(timedelta(hours=2)))
        await self._test_val_result("SELECT $1 -- no-cache timetz", val, val)
        val = time(13, 12, tzinfo=ZoneInfo("Europe/Paris"))
        await self._test_val_result(
            "SELECT $1 -- no-cache timetz 2", time(13, 12), val)
        val = time(13, 12, tzinfo=timezone(timedelta(hours=-3)))
        await self._test_val_result("SELECT $1 -- no-cache timetz 3", val, val)
        val = time(13, 12, tzinfo=timezone(timedelta(hours=-18)))
        await self._test_val_result(
            "SELECT $1 -- no-cache timetz 4", "13:12:00-18:00", val)
        val = time(13, 12, tzinfo=timezone(timedelta(hours=16)))
        await self._test_val_result(
            "SELECT $1 -- no-cache timetz 5", "13:12:00+16:00", val)

    async def test_jsonb_result(self):
        await self._test_val_result(
            "SELECT '{\"hello\": \"world\"}'::jsonb",
            {"hello": "world"})

    async def test_json_result(self):
        await self._test_val_result(
            "SELECT '{\"hello\": \"world\"}'::json",
            {"hello": "world"})

    async def test_jsonb_param(self):
        val = {"key_1": "value", "key_2": 13, "key_3": None}
        await self._test_val_result("SELECT $1", val, PGJson(val))


class PyConnTypeCase(ConnTypeCase):
    @classmethod
    def setUpClass(cls) -> None:
        async_connection.AsyncPGProtocol = async_protocol.PyAsyncPGProtocol

    @classmethod
    def tearDownClass(cls) -> None:
        async_connection.AsyncPGProtocol = async_protocol.AsyncPGProtocol
