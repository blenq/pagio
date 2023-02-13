from datetime import date, datetime, timezone, timedelta, time
from decimal import Decimal
from ipaddress import (
    IPv4Interface, IPv6Interface, IPv4Network, IPv6Network, IPv4Address,
    IPv6Address)
import unittest
from uuid import UUID, uuid4

from pagio import (
    Connection, sync_connection, sync_protocol, Format, ServerError, PGJson,
    ServerWarning, PGTextArray, PGInt4Range, PGInt8Range, PGText, PGNumRange,
    PGTimestampTZRange, PGDateRange, PGRegConfig, ServerNotice,
    PGInt4MultiRange, PGInt8MultiRange, PGNumMultiRange,
)
from pagio.types import txt_hstore_to_python, bin_hstore_to_python
from pagio.zoneinfo import ZoneInfo


class ConnTypeCase(unittest.TestCase):

    def setUp(self) -> None:
        self._cn = Connection(database="postgres")

    def tearDown(self) -> None:
        self._cn.close()

    def test_varchar_create(self):
        self._cn.execute("CREATE TEMPORARY TABLE yo (m VARCHAR(10))")
        self._cn.execute("INSERT INTO yo VALUES ($1)", "")
        self.assertEqual(self._cn.execute("SELECT * FROM yo")[0][0], "")

    def _test_val_result(self, sql, val, *params):
        res = self._cn.execute(sql, *params)
        self.assertEqual(val, res[0][0])
        res = self._cn.execute(sql, *params, result_format=Format.TEXT)
        self.assertEqual(val, res[0][0])
        res = self._cn.execute(sql, *params, result_format=Format.BINARY)
        self.assertEqual(val, res[0][0])

    def test_concat_str(self):
        self._test_val_result(
            "SELECT CONCAT('hello', $1)", "hello@", PGText("@"))

    def test_ipv4_inet_result(self):
        self._test_val_result(
            "SELECT '192.168.0.1'::inet", IPv4Interface("192.168.0.1"))
        self._test_val_result(
            "SELECT '192.168.0.1/24'::inet", IPv4Interface("192.168.0.1/24"))

    def test_inet_array_result(self):
        self._test_val_result(
            "SELECT '{192.168.0.1, ::1}'::inet[]",
            [IPv4Interface("192.168.0.1"), IPv6Interface("::1")])

    def test_ipv4_inet_param(self):
        val = IPv4Interface("192.168.0.1")
        self._test_val_result("SELECT $1 -- no-cache 3", val, val)
        val = IPv4Interface("192.168.0.1/24")
        self._test_val_result("SELECT $1 -- no-cache 4", val, val)
        val = IPv4Address("192.168.0.10")
        self._test_val_result(
            "SELECT $1 -- no-cache 5", IPv4Interface("192.168.0.10/32"), val)

    def test_ipv6_inet_result(self):
        self._test_val_result(
            "SELECT '2001:db8:85a3:0:0:8a2e:370:7334'::inet",
            IPv6Interface("2001:db8:85a3:0:0:8a2e:370:7334"))
        self._test_val_result(
            "SELECT '2001:db8:85a3:0:0:8a2e:370:7334/64'::inet",
            IPv6Interface("2001:db8:85a3:0:0:8a2e:370:7334/64"))

    def test_ipv6_inet_param(self):
        val = IPv6Interface("2001:db8:85a3:0:0:8a2e:370:7334")
        self._test_val_result("SELECT $1 -- no-cache 6", val, val)
        val = IPv6Interface("2001:db8:85a3:0:0:8a2e:370:7334/64")
        self._test_val_result("SELECT $1 -- no-cache 7", val, val)
        val = IPv6Address("2001:db8:85a3:0:0:8a2e:370:7334")
        self._test_val_result(
            "SELECT $1 -- no-cache 8",
            IPv6Interface("2001:db8:85a3:0:0:8a2e:370:7334"), val)

    def test_ipv4_cidr_result(self):
        self._test_val_result(
            "SELECT '192.168.0.0/24'::cidr", IPv4Network("192.168.0.0/24"))

    def test_ipv4_cidr_array_result(self):
        self._test_val_result(
            "SELECT ARRAY['192.168.0.0/24']::cidr[]",
            [IPv4Network("192.168.0.0/24")])

    def test_ipv4_cidr_param(self):
        val = IPv4Network('192.168.0.0/24')
        self._test_val_result("SELECT $1 -- no-cache 9", val, val)

    def test_ipv6_cidr_result(self):
        self._test_val_result(
            "SELECT '2001:db8:85a3:0:0:8a2e:0:0/96'::cidr",
            IPv6Network("2001:db8:85a3:0:0:8a2e:0:0/96"))

    def test_ipv6_cidr_param(self):
        val = IPv6Network('2001:db8:85a3:0:0:8a2e:0:0/96')
        self._test_val_result("SELECT $1 -- no-cache 10", val, val)

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
            "SELECT '123.456'::numeric", Decimal("123.456"))
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
        if int(self._cn.server_parameters["server_version"].split(".", 1)[0]) >= 14:
            self._test_numeric_val(
                "SELECT 'inf'::numeric", Decimal("inf"))
            self._test_numeric_val(
                "SELECT '-inf'::numeric", Decimal("-inf"))

    def test_numeric_array_result(self):
        self._test_val_result(
            "SELECT ARRAY['123.456', 'inf']::numeric[]",
            [Decimal("123.456"), Decimal('inf')])

    def test_numeric_range_result(self):
        self._test_val_result(
            "SELECT '[10,11)'::numrange;", PGNumRange(Decimal(10), 11))
        self._test_val_result(
            "SELECT '[,)'::numrange;", PGNumRange(None, None))
        self._test_val_result(
            "SELECT 'empty'::numrange;", PGNumRange.empty())

    def test_numeric_multirange_result(self):
        self._test_val_result(
            "SELECT '{[10,11)}'::nummultirange;",
            PGNumMultiRange((Decimal(10), 11)))
        self._test_val_result(
            "SELECT '{[,)}'::nummultirange;", PGNumMultiRange((None, None)))
        self._test_val_result(
            "SELECT '{empty}'::nummultirange;", PGNumMultiRange())
        self._test_val_result(
            "SELECT '{[10,11),(13,15]}'::nummultirange;",
            PGNumMultiRange((Decimal(10), 11), (13, 15, '(]')))

    def test_numeric_range_array_result(self):
        self._test_val_result(
            "SELECT ARRAY['[10,11)']::numrange[];", [PGNumRange(10, 11)])

    def test_numeric_param(self):
        for val_str in [
            "12.34", "Infinity", "inf", "-infinity", "-inf", "1.234e123",
            "8.7654e-765", "12345.67", "123.4567", "1234.567", "8.7654e-764",
            "1000", "1E-16383", "1E+131071",
        ]:
            val = Decimal(val_str)
            self._test_val_result("SELECT $1 -- no-cache 11", val, val)
        for val_str in ["Nan", "-NaN", "sNaN", "-sNaN"]:
            val = Decimal(val_str)
            res = self._cn.execute(f"SELECT $1 -- no-cache val_str", val)
            self.assertTrue(res.rows[0][0].is_nan())

        # Out of range tests.
        res = self._cn.execute("SELECT $1 -- oor 1", Decimal("1E-16384"))
        self.assertEqual("1E-16384", res.rows[0][0])
        res = self._cn.execute("SELECT $1 -- oor 2", Decimal("1E+131072"))
        self.assertEqual("1E+131072", res.rows[0][0])

        # Should be out of range on server. If not, adjust code to new
        # situation
        with self.assertRaises(ServerError) as cm:
            self._cn.execute("SELECT '1E-16384'::numeric;")
        self.assertEqual(cm.exception.code, "22003")
        with self.assertRaises(ServerError) as cm:
            self._cn.execute("SELECT '1E+131072'::numeric;")
        self.assertEqual(cm.exception.code, "22003")

    def test_bytea_result(self):
        self._cn.execute("SET bytea_output TO 'hex'")
        self._test_val_result("SELECT '\\x09686f695c'::bytea", b'\thoi\\')
        self._cn.execute("SET bytea_output TO 'escape'")
        self._test_val_result("SELECT '\\x09686f695c'::bytea", b'\thoi\\')

    def test_bytea_array_result(self):
        self._cn.execute("SET bytea_output TO 'hex'")
        self._test_val_result(
            "SELECT ARRAY['\\x09686f695c']::bytea[]", [b'\thoi\\'])

    def test_bytea_param(self):
        val = b'\thoi\\'
        self._test_val_result("SELECT $1 -- no-cache 11", val, val)

    def test_uuid_result(self):
        self._test_val_result(
            "SELECT '42d36a04-8ff1-4337-870e-51de61b19771'::uuid",
            UUID('42d36a04-8ff1-4337-870e-51de61b19771'))

    def test_uuid_param_result(self):
        self._test_val_result(
            "SELECT ARRAY['42d36a04-8ff1-4337-870e-51de61b19771']::uuid[]",
            [UUID('42d36a04-8ff1-4337-870e-51de61b19771')])

    def test_uuid_param(self):
        val = uuid4()
        self._test_val_result("SELECT $1 -- no-cache 13", val, val)

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

    def test_date_array_result(self):
        self._test_val_result(
            "SELECT ARRAY['2021-03-15', '0900-03-15']::date[]",
            [date(2021, 3, 15), date(900, 3, 15)])

    def test_date_range_result(self):
        self._test_val_result(
            "SELECT '[0900-03-15, 2021-03-15]'::daterange",
            PGDateRange(date(900, 3, 15), date(2021, 3, 15), "[]"))

    def test_date_range_array_result(self):
        self._test_val_result(
            "SELECT ARRAY['[0900-03-15, 2021-03-15]']::daterange[]",
            [PGDateRange(date(900, 3, 15), date(2021, 3, 15), "[]")])

    def test_date_param(self):
        val = date(2021, 3, 15)
        self._test_val_result("SELECT $1 -- no-cache 14", val, val)
        val = date(1980, 3, 15)
        self._test_val_result("SELECT $1 -- no-cache 15", val, val)
        val = date(1980, 1, 15)
        self._test_val_result("SELECT $1 -- no-cache 16", val, val)
        val = date(2000, 3, 15)
        self._test_val_result("SELECT $1 -- no-cache 17", val, val)
        val = date(1, 1, 1)
        self._test_val_result("SELECT $1 -- no-cache 18", val, val)
        val = date(9999, 12, 31)
        self._test_val_result("SELECT $1 -- no-cache 19", val, val)

    def test_timestamp_result(self):
        self._test_val_result(
            "SELECT '0900-03-15 14:10:03'::timestamp",
            datetime(900, 3, 15, 14, 10, 3))
        self._test_val_result(
            "SELECT '2021-03-15 14:10:03'::timestamp",
            datetime(2021, 3, 15, 14, 10, 3))
        self._test_val_result(
            "SELECT '2021-03-15 14:10:03.2'::timestamp",
            datetime(2021, 3, 15, 14, 10, 3, 200000))
        self._test_val_result(
            "SELECT '2021-03-15 14:10:03.02'::timestamp",
            datetime(2021, 3, 15, 14, 10, 3, 20000))
        self._test_val_result(
            "SELECT '2021-03-15 14:10:03.002'::timestamp",
            datetime(2021, 3, 15, 14, 10, 3, 2000))
        self._test_val_result(
            "SELECT '2021-03-15 14:10:03.0002'::timestamp",
            datetime(2021, 3, 15, 14, 10, 3, 200))
        self._test_val_result(
            "SELECT '2021-03-15 14:10:03.000002'::timestamp",
            datetime(2021, 3, 15, 14, 10, 3, 2))
        self._test_val_result(
            "SELECT '20210-03-15 14:10:03'::timestamp",
            '20210-03-15 14:10:03')
        self._test_val_result(
            "SELECT '2021-03-15 14:10:03 BC'::timestamp",
            '2021-03-15 14:10:03 BC')
        self._test_val_result(
            "SELECT '0002-03-15 14:10:03 BC'::timestamp",
            '0002-03-15 14:10:03 BC')
        self._test_val_result(
            "SELECT 'infinity'::timestamp", 'infinity')
        self._test_val_result(
            "SELECT '-infinity'::timestamp", '-infinity')
        self._cn.execute("SET DateStyle TO postgres, dmy")
        res = self._cn.execute(
            "SELECT '2021-03-15 14:10:03'::timestamp",
            result_format=Format.TEXT)
        self.assertEqual("Mon 15 Mar 14:10:03 2021", res[0][0])
        res = self._cn.execute(
            "SELECT '2021-03-15 14:10:03'::timestamp",
            result_format=Format.BINARY)
        self.assertEqual(datetime(2021, 3, 15, 14, 10, 3), res[0][0])

    def test_timestamp_param(self):
        val = datetime(2021, 3, 15, 14, 10, 3)
        self._test_val_result("SELECT $1 -- no-cache 19", val, val)
        val = datetime(2021, 3, 15, 14, 10, 3, 234)
        self._test_val_result("SELECT $1 -- no-cache 20", val, val)

    def test_timestamp_array_result(self):
        self._test_val_result(
            "SELECT ARRAY["
            "    '0900-03-15 14:10:03', '2021-03-15 14:10:03']::timestamp[]",
            [datetime(900, 3, 15, 14, 10, 3), datetime(2021, 3, 15, 14, 10, 3)]
        )

    def _test_tstz_val(self, sql, val):
        res = self._cn.execute(sql)
        res_val = res[0][0]
        self.assertEqual(val, res_val)
        self.assertEqual(val.tzinfo, res_val.tzinfo)
        res = self._cn.execute(sql, result_format=Format.TEXT)
        res_val = res[0][0]
        self.assertEqual(val, res_val)
        self.assertEqual(val.tzinfo, res_val.tzinfo)
        res = self._cn.execute(sql, result_format=Format.BINARY)
        res_val = res[0][0]
        self.assertEqual(val, res_val)
        if self._cn.tzinfo is not None:
            self.assertEqual(
                val.tzinfo.utcoffset(val.replace(tzinfo=None)),
                res_val.tzinfo.utcoffset(res_val.replace(tzinfo=None)))

    def test_timestamptz_result(self):
        self._cn.execute("SET TIMEZONE TO 'Europe/Berlin'")
        self._test_tstz_val(
            "SELECT '2021-03-15 14:10:03'::timestamptz", datetime(
                2021, 3, 15, 14, 10, 3, tzinfo=ZoneInfo("Europe/Berlin")))
        self._cn.execute("SET TIMEZONE TO 'Europe/Berlin'")
        self._test_tstz_val(
            "SELECT '2021-03-15 14:10:03.2'::timestamptz", datetime(
                2021, 3, 15, 14, 10, 3, 200000,
                tzinfo=ZoneInfo("Europe/Berlin")))
        self._test_tstz_val(
            "SELECT '2021-03-15 14:10:03.02'::timestamptz", datetime(
                2021, 3, 15, 14, 10, 3, 20000,
                tzinfo=ZoneInfo("Europe/Berlin")))
        self._test_tstz_val(
            "SELECT '2021-03-15 14:10:03.002'::timestamptz", datetime(
                2021, 3, 15, 14, 10, 3, 2000,
                tzinfo=ZoneInfo("Europe/Berlin")))
        self._test_tstz_val(
            "SELECT '2021-03-15 14:10:03.0002'::timestamptz", datetime(
                2021, 3, 15, 14, 10, 3, 200, tzinfo=ZoneInfo("Europe/Berlin")))
        self._test_tstz_val(
            "SELECT '2021-03-15 14:10:03.00002'::timestamptz", datetime(
                2021, 3, 15, 14, 10, 3, 20, tzinfo=ZoneInfo("Europe/Berlin")))
        self._test_tstz_val(
            "SELECT '2021-03-15 14:10:03.000002'::timestamptz", datetime(
                2021, 3, 15, 14, 10, 3, 2, tzinfo=ZoneInfo("Europe/Berlin")))
        self._cn.execute("SET TIMEZONE TO '-02:30'")
        self._test_tstz_val(
            "SELECT '2021-03-15 14:11:03'::timestamptz", datetime(
                2021, 3, 15, 14, 11, 3, tzinfo=timezone(
                    timedelta(hours=2, minutes=30))))
        res = self._cn.execute(
            "SELECT '2021-03-15 14:10:03 BC'::timestamptz",
            result_format=Format.TEXT)
        self.assertEqual('2021-03-15 14:10:03+02:30 BC', res.rows[0][0])
        res = self._cn.execute(
            "SELECT '2021-03-15 14:10:03 BC'::timestamptz",
            result_format=Format.BINARY)
        self.assertEqual('2021-03-15 11:40:03+00 BC', res.rows[0][0])
        self._test_val_result(
            "SELECT 'infinity'::timestamptz", 'infinity')
        self._test_val_result(
            "SELECT '-infinity'::timestamptz", '-infinity')
        self._cn.execute("SET TIMEZONE TO 'America/Chicago'")
        self._cn.execute("SET DateStyle TO postgres, dmy")
        res = self._cn.execute(
            "SELECT '2021-03-15 14:10:04'::timestamptz",
            result_format=Format.TEXT)
        self.assertEqual("Mon 15 Mar 14:10:04 2021 CDT", res[0][0])
        res = self._cn.execute(
            "SELECT '2021-03-15 14:10:05'::timestamptz",
            result_format=Format.BINARY)
        self.assertEqual(datetime(2021, 3, 15, 14, 10, 5, tzinfo=ZoneInfo("America/Chicago")), res[0][0])
        self._cn.execute("SET DateStyle TO ISO, YMD")
        res = self._cn.execute(
            "SELECT '0002-03-15 14:10:06.12 BC'::timestamptz",
            result_format=Format.TEXT)
        self.assertEqual(res.rows[0][0], '0002-03-15 14:10:06.12-05:50:36 BC')
        res = self._cn.execute(
            "SELECT '0002-03-15 14:10:07 BC'::timestamptz",
            result_format=Format.BINARY)
        self.assertEqual(res.rows[0][0], '0002-03-15 20:00:43+00 BC')
        self._test_tstz_val(
            "SELECT '0002-03-15 14:10:08'::timestamptz",
            datetime(2, 3, 15, 14, 10, 8, tzinfo=ZoneInfo("America/Chicago"))
        )

    def test_timestamptz_array_result(self):
        self._test_val_result(
            "SELECT ARRAY["
            "    '0900-03-15 14:10:03', '2021-03-15 14:10:03']::timestamptz[]",
            [datetime(900, 3, 15, 14, 10, 3, tzinfo=self._cn.tzinfo),
             datetime(2021, 3, 15, 14, 10, 3, tzinfo=self._cn.tzinfo)]
        )

    def test_timestamptz_range_result(self):
        self._test_val_result(
            "SELECT '[0900-03-15 14:10:03, 2021-03-15 14:10:03)'::tstzrange",
            PGTimestampTZRange(
                datetime(900, 3, 15, 14, 10, 3, tzinfo=self._cn.tzinfo),
                datetime(2021, 3, 15, 14, 10, 3, tzinfo=self._cn.tzinfo)))

    def test_timestamptz_range_array_result(self):
        self._test_val_result(
            "SELECT ARRAY['[0900-03-15 14:10:03, 2021-03-15 14:10:03)']::tstzrange[]",
            [PGTimestampTZRange(
                datetime(900, 3, 15, 14, 10, 3, tzinfo=self._cn.tzinfo),
                datetime(2021, 3, 15, 14, 10, 3, tzinfo=self._cn.tzinfo))])

    def test_timestamptz_param(self):

        self._cn.execute("SET TIMEZONE TO 'America/Chicago'")
        res = self._cn.execute(
            "SELECT $1::timestamptz -- no-cache 3", datetime.max, result_format=Format.BINARY)
        db_val = res.rows[0][0]
        db_val = db_val.replace(tzinfo=None)
        self.assertEqual(datetime.max, db_val)
        res = self._cn.execute(
            "SELECT $1::timestamptz -- no-cache 4", datetime.max, result_format=Format.TEXT)
        db_val = res.rows[0][0]
        db_val = db_val.replace(tzinfo=None)
        self.assertEqual(datetime.max, db_val)

        self._cn.execute("SET TIMEZONE TO 'Europe/Amsterdam'")
        val = datetime(2021, 3, 15, 14, 10, 3)
        res = self._cn.execute(
            "SELECT $1::timestamptz", val, result_format=Format.TEXT)
        tz2 = timezone(timedelta(hours=1))
        val2 = datetime(2021, 3, 15, 14, 10, 3, tzinfo=tz2)
        db_val = res.rows[0][0]
        self.assertEqual(val2, db_val)
        self.assertEqual(ZoneInfo("Europe/Amsterdam"), db_val.tzinfo)

        res = self._cn.execute(
            "SELECT $1::timestamptz", val, result_format=Format.BINARY)
        db_val = res.rows[0][0]
        self.assertEqual(val2, db_val)
        self.assertEqual(val2, db_val)
        self.assertEqual(ZoneInfo("Europe/Amsterdam"), db_val.tzinfo)

        res = self._cn.execute(
            "SELECT $1 -- no-cache 1", val2, result_format=Format.TEXT)
        db_val = res.rows[0][0]
        self.assertEqual(val2, db_val)
        self.assertEqual(ZoneInfo("Europe/Amsterdam"), db_val.tzinfo)

        res = self._cn.execute(
            "SELECT $1 -- no-cache 2", val2, result_format=Format.BINARY)
        db_val = res.rows[0][0]
        self.assertEqual(val2, db_val)
        self.assertEqual(ZoneInfo("Europe/Amsterdam"), db_val.tzinfo)

    def test_time_result(self):
        self._test_val_result("SELECT '13:12'::time", time(13, 12))
        self._test_val_result(
            "SELECT '13:12:34.23'::time", time(13, 12, 34, 230000))
        self._test_val_result("SELECT '24:00'::time", time(0))

    def test_time_array_result(self):
        self._test_val_result(
            "SELECT ARRAY['13:12', NULL]::time[]", [time(13, 12), None])

    def test_time_param(self):
        val = time(13, 12)
        self._test_val_result("SELECT $1 -- no-cache time", val, val)

    def test_timetz_result(self):
        self._test_val_result(
            "SELECT '13:12+01'::timetz",
            time(13, 12, tzinfo=timezone(timedelta(hours=1))))
        self._test_val_result(
            "SELECT '13:12:22-02:30'::timetz",
            time(13, 12, 22, tzinfo=timezone(
                -1 * timedelta(hours=2, minutes=30))))
        self._test_val_result(
            "SELECT '13:12:22+02:30:12'::timetz",
            time(13, 12, 22, tzinfo=timezone(
                timedelta(hours=2, minutes=30, seconds=12))))
        self._test_val_result(
            "SELECT '24:00:00-02:30:12'::timetz",
            time(0, tzinfo=timezone(
                -1 * timedelta(hours=2, minutes=30, seconds=12))))

    def test_timetz_array_result(self):
        self._test_val_result(
            "SELECT ARRAY['13:12+01']::timetz[]",
            [time(13, 12, tzinfo=timezone(timedelta(hours=1)))])

    def test_timetz_param(self):
        val = time(13, 12, tzinfo=timezone(timedelta(hours=2)))
        self._test_val_result("SELECT $1 -- no-cache timetz", val, val)
        val = time(13, 12, tzinfo=ZoneInfo("Europe/Paris"))
        self._test_val_result(
            "SELECT $1 -- no-cache timetz 2", time(13, 12), val)
        val = time(13, 12, tzinfo=timezone(timedelta(hours=-3)))
        self._test_val_result("SELECT $1 -- no-cache timetz 3", val, val)
        val = time(13, 12, tzinfo=timezone(timedelta(hours=-18)))
        self._test_val_result(
            "SELECT $1 -- no-cache timetz 4", "13:12:00-18:00", val)
        val = time(13, 12, tzinfo=timezone(timedelta(hours=16)))
        self._test_val_result(
            "SELECT $1 -- no-cache timetz 5", "13:12:00+16:00", val)
        val = time(13, 12, tzinfo=timezone(timedelta(hours=2, microseconds=10)))
        self._test_val_result("SELECT $1 -- no-cache timetz 6", val, val)

    def test_jsonb_result(self):
        self._test_val_result(
            "SELECT '{\"hello\": \"world\"}'::jsonb",
            {"hello": "world"})

    def test_json_result(self):
        self._test_val_result(
            "SELECT '{\"hello\": \"world\"}'::json",
            {"hello": "world"})

    def test_jsonb_param(self):
        val = {"key_1": "value", "key_2": 13, "key_3": None}
        self._test_val_result("SELECT $1", val, PGJson(val))

    def test_int2_array_result(self):
        self._test_val_result(
            "SELECT '{{1, 2, 3}, {4, 5, 6}}'::int2[]",
            [[1, 2, 3], [4, 5, 6]])
        self._test_val_result("SELECT '{}'::int2[]", [])
        self._test_val_result(
            "SELECT '{{1, NULL, 3}, {4, 5, 6}}'::int2[]",
            [[1, None, 3], [4, 5, 6]])

    def test_int4_array_result(self):
        self._test_val_result(
            "SELECT '{{1, 2, 3}, {4, 5, 6}}'::int4[]",
            [[1, 2, 3], [4, 5, 6]])
        self._test_val_result("SELECT '{}'::int4[]", [])
        self._test_val_result(
            "SELECT '{{1, NULL, 3}, {4, 5, 6}}'::int4[]",
            [[1, None, 3], [4, 5, 6]])

    def test_int8_array_result(self):
        self._test_val_result(
            "SELECT '{{1, 2, 3}, {4, 5, 6}}'::int8[]",
            [[1, 2, 3], [4, 5, 6]])
        self._test_val_result("SELECT '{}'::int8[]", [])
        self._test_val_result(
            "SELECT '{{1, NULL, 3}, {4, 5, 6}}'::int8[]",
            [[1, None, 3], [4, 5, 6]])

    def test_float4_array_result(self):
        for fmt in [Format.TEXT, Format.BINARY]:
            res = self._cn.execute(
                "SELECT '{1.2, 2.34, NULL, 3.456}'::float4[]",
                result_format=fmt)
            for v1, v2 in zip(res.rows[0][0], [1.2, 2.34, None, 3.456]):
                if v2 is None:
                    self.assertIsNone(v1)
                else:
                    # float4 to float8 conversion results in small differences
                    self.assertAlmostEqual(v1, v2, places=6)

    def test_float8_array_result(self):
        for fmt in [Format.TEXT, Format.BINARY]:
            res = self._cn.execute(
                "SELECT '{1.2, 2.34, NULL, 3.456}'::float8[]",
                result_format=fmt
            )
            self.assertEqual(res.rows[0][0], [1.2, 2.34, None, 3.456])

    def test_text_array_result(self):
        self._test_val_result(
            "SELECT ARRAY['hi', NULL, 'h\"o', 'h}o', 'h,o'];",
            ['hi', None, 'h"o', 'h}o', 'h,o']
        )
        self._test_val_result(
            "SELECT ARRAY['hi', NULL, 'h\"o', 'h}o', 'h,o']::varchar[];",
            ['hi', None, 'h"o', 'h}o', 'h,o']
        )
        self._test_val_result(
            "SELECT ARRAY['hio', NULL, 'h\"o', 'h}o', 'h,o']::char(3)[];",
            ['hio', None, 'h"o', 'h}o', 'h,o']
        )
        self._test_val_result(
            "SELECT ARRAY['h', NULL, '\"', '}', ',']::\"char\"[];",
            ['h', None, '"', '}', ',']
        )
        self._test_val_result(
            "SELECT ARRAY['hi', NULL, 'h\"o', 'h}o', 'h,o']::name[];",
            ['hi', None, 'h"o', 'h}o', 'h,o']
        )

    def test_text_array_param(self):
        val = ["hoi", "hi"]
        self._test_val_result("SELECT $1", val, PGTextArray(val))
        val = ["hoi", None]
        self._test_val_result("SELECT $1", val, PGTextArray(val))

    def test_bool_array_result(self):
        self._test_val_result(
            "SELECT ARRAY['true', NULL, 'false']::bool[];",
            [True, None, False]
        )

    def test_jsonb_array_result(self):
        self._test_val_result(
            "SELECT ARRAY['{\"key\": 3}', NULL, 'false']::jsonb[];",
            [{"key": 3}, None, False]
        )

    def test_notice_response(self):
        with self.assertWarns(ServerWarning):
            # COMMIT without BEGIN returns a warning
            self._cn.execute("COMMIT")

        self._cn.execute(
            "CREATE TEMPORARY TABLE test1 (id int NOT NULL PRIMARY KEY);"
            "CREATE TEMPORARY TABLE test2 ("
            "   id int REFERENCES test1(id) ON DELETE CASCADE)"
        )
        with self.assertWarns(ServerNotice):
            # TRUNCATE CASCADE returns a notice
            self._cn.execute("TRUNCATE test1 CASCADE")

        self._cn.execute("DROP TABLE test2; DROP TABLE test1")

    def test_interval_result(self):
        self._cn.execute("SET IntervalStyle TO postgres")
        self._test_val_result(
            "SELECT '2 year -1 month -3 days 04:05:06'::interval;",
            (23, timedelta(days=-3, hours=4, minutes=5, seconds=6)))
        self._test_val_result(
            "SELECT '2 year -1 month -3 days 23:59:59.999999'::interval;",
            (23, timedelta(
                -3, hours=23, minutes=59, seconds=59, microseconds=999999)))
        self._test_val_result(
            "SELECT '-2 year 4 mons'::interval", (-20, timedelta(0)))
        self._test_val_result(
            "SELECT '23 days -04:05:06'::interval;",
            (0, timedelta(days=23, hours=-4, minutes=-5, seconds=-6)))
        self._test_val_result(
            "SELECT '-04:05:06'::interval;",
            (0, timedelta(hours=-4, minutes=-5, seconds=-6)))
        # self._cn.execute("SET IntervalStyle TO sql_standard")
        # self._test_val_result(
        #     "SELECT '2 year -1 month -3 days 04:05:06'::interval;",
        #     "+1-11 -3 +4:05:06")

    def test_interval_param(self):
        val = timedelta(days=2, seconds=13, milliseconds=300)
        self._test_val_result("SELECT $1", (0, val), val)
        val = -1 * val
        self._test_val_result("SELECT $1", (0, val), val)

    def test_interval_array_result(self):
        self._cn.execute("SET IntervalStyle TO postgres")
        self._test_val_result(
            "SELECT ARRAY["
            "    '2 year -1 month -3 days 04:05:06',"
            "    '-2 year 4 mons']::interval[];",
            [(23, timedelta(days=-3, hours=4, minutes=5, seconds=6)),
             (-20, timedelta(0))])

    def test_xml_result(self):
        self._test_val_result(
            "SELECT '<hi>hello</hi>'::xml", "<hi>hello</hi>")

    def test_xml_array_result(self):
        self._test_val_result(
            "SELECT ARRAY['<hi>hello</hi>']::xml[]", ["<hi>hello</hi>"])

    def test_int4_range_result(self):
        self._test_val_result(
            "SELECT '[10,11)'::int4range;", PGInt4Range(10, 11))
        self._test_val_result(
            "SELECT '[,)'::int4range;", PGInt4Range(None, None))
        self._test_val_result(
            "SELECT 'empty'::int4range;", PGInt4Range.empty())

    def test_int4_range_array_result(self):
        self._test_val_result(
            "SELECT ARRAY['[10, 2147483646]']::int4range[];",
            [PGInt4Range(10, 2147483647)])

    def test_int4_multirange_result(self):
        self._test_val_result(
            "SELECT '{[10,11),[15,20)}'::int4multirange;",
            PGInt4MultiRange((10, 11), (15, 20)))
        self._test_val_result(
            "SELECT '{empty}'::int4multirange;",
            PGInt4MultiRange())
        self._test_val_result(
            "SELECT '{(,5),[10,15),[20,)}'::int4multirange;",
            PGInt4MultiRange((None, 5), (10, 15), (20, None)))

    def test_int8_range_result(self):
        self._test_val_result(
            "SELECT '[10,11)'::int8range;", PGInt8Range(10, 11))
        self._test_val_result(
            "SELECT '[,)'::int8range;", PGInt8Range(None, None))
        self._test_val_result(
            "SELECT 'empty'::int8range;", PGInt8Range.empty())

    def test_int8_range_array_result(self):
        self._test_val_result(
            "SELECT ARRAY['[10, 2147483646]']::int8range[];",
            [PGInt8Range(10, 2147483647)])

    def test_int8_multirange_result(self):
        self._test_val_result(
            "SELECT '{[10,11),[15,20)}'::int8multirange;",
            PGInt8MultiRange((10, 11), (15, 20)))
        self._test_val_result(
            "SELECT '{empty}'::int8multirange;",
            PGInt8MultiRange())
        self._test_val_result(
            "SELECT '{(,5),[10,15),[20,)}'::int8multirange;",
            PGInt8MultiRange((None, 5), (10, 15), (20, None)))

    def test_hstore_result(self):
        res = self._cn.execute(
            "SELECT EXISTS(SELECT datname FROM pg_database "
            "WHERE datname='pagio_test');")
        if not res[0][0]:
            self._cn.execute("CREATE DATABASE pagio_test")
        with Connection(database="pagio_test") as tcn:
            tcn.execute("CREATE EXTENSION IF NOT EXISTS hstore")
            res = tcn.execute(
                "SELECT oid, typarray FROM pg_type WHERE typname = 'hstore';")
            hstore_oid, hstore_array_oid = res[0]
            tcn.register_res_converter(
                hstore_oid, txt_hstore_to_python, bin_hstore_to_python,
                hstore_array_oid)
            res = tcn.execute(
                'SELECT \'"hi"=>"wow", "hello"=>NULL, "last"=>"no"\'::hstore;',
                result_format=Format.TEXT)
            self.assertEqual(
                res[0][0], {"hi": "wow", "hello": None, "last": "no"})
            res = tcn.execute(
                'SELECT \'"hi"=>"wow", "hello"=>NULL, "last"=>"no"\'::hstore;',
                result_format=Format.BINARY)
            self.assertEqual(
                res[0][0], {"hi": "wow", "hello": None, "last": "no"})
            tcn.execute("DROP EXTENSION hstore")
        self._cn.execute("DROP DATABASE pagio_test")

    def test_regconfig_result(self):
        res = self._cn.execute(
            "SELECT 'english'::regconfig", result_format=Format.TEXT)
        self.assertEqual(res[0][0], 'english')

    def test_regconfig_param(self):
        res = self._cn.execute(
            "SELECT $1", PGRegConfig("english"), result_format=Format.TEXT)
        self.assertEqual(res[0][0], 'english')
        res = self._cn.execute(
            "SELECT to_tsvector($1::regconfig, "
            "'a fat  cat sat on a mat - it ate a fat rats')",
            PGRegConfig("english"), result_format=Format.TEXT)
        self.assertEqual(res[0][0], "'ate':9 'cat':3 'fat':2,11 'mat':7 'rat':12 'sat':4")


class PyConnTypeCase(ConnTypeCase):
    @classmethod
    def setUpClass(cls) -> None:
        sync_connection.PGProtocol = sync_protocol.PyPGProtocol

    @classmethod
    def tearDownClass(cls) -> None:
        sync_connection.PGProtocol = sync_protocol.PGProtocol
