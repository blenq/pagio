import struct
try:
    from unittest import IsolatedAsyncioTestCase
except ImportError:
    from later.unittest.backport.async_case import IsolatedAsyncioTestCase

from pagio import AsyncConnection, Format, ServerError, ProtocolStatus


class ResultCase(IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self._cn = await AsyncConnection(database="postgres")

    async def test_select(self):
        res = await self._cn.execute("SELECT '12'")
        self.assertEqual(res[0][0], '12')

    async def test_large_buffer(self):
        res = await self._cn.execute("SELECT rpad('', 32000, '0');")
        self.assertEqual(res[0][0], '0' * 32000)

    async def test_select_iter(self):
        res = await self._cn.execute(
            "SELECT '12' UNION SELECT '13' ORDER BY 1")
        i = 12
        for row in res:
            self.assertIsInstance(row, tuple)
            self.assertEqual(row[0], str(i))
            i += 1

    async def test_select_none_param(self):
        res = await self._cn.execute("SELECT $1", None)
        self.assertEqual(res[0], (None,))
        self.assertEqual(res.fields[0].type_oid, 25)
        self.assertEqual(res.fields[0][3], 25)

    async def test_select_int(self):
        res = await self._cn.execute("SELECT 12, 12::int4, 12::int8, 12::int2")
        self.assertEqual(res[0], (12, 12, 12, 12))
        res = await self._cn.execute(
            "SELECT 12, 12::int4, 12::int8, 12::int2",
            result_format=Format.BINARY)
        self.assertEqual(res[0], (12, 12, 12, 12))

    async def test_select_float(self):
        res = await self._cn.execute("SELECT 1.5::float4, 1.5::float8")
        self.assertEqual(res[0], (1.5, 1.5))
        res = await self._cn.execute(
            "SELECT 1.5::float4, 1.5::float8", result_format=Format.BINARY)
        self.assertEqual(res[0], (1.5, 1.5))

    async def test_select_float_param(self):
        res = await self._cn.execute("SELECT $1", 1.5)
        self.assertEqual(res[0], (1.5,))

    async def test_select_bool(self):
        res = await self._cn.execute("SELECT true, false")
        self.assertEqual(res[0], (True, False))
        res = await self._cn.execute(
            "SELECT true, false", result_format=Format.BINARY)
        self.assertEqual(res[0], (True, False))

    async def test_select_bool_param(self):
        res = await self._cn.execute("SELECT $1, $2", True, False)
        self.assertEqual(res[0], (True, False))

    async def _test_select_typ(self, sql, val):
        res = await self._cn.execute(sql)
        self.assertEqual(res[0], (val,))
        res = await self._cn.execute(sql, result_format=Format.BINARY)
        self.assertEqual(res[0], (val,))

    async def test_select_text(self):
        await self._test_select_typ("SELECT 'hel''lo €'", "hel'lo €")

    async def test_select_string_param(self):
        res = await self._cn.execute("SELECT $1", "hello")
        self.assertEqual(res[0], ("hello",))
        res = await self._cn.execute(
            "SELECT $1", "hello", result_format=Format.BINARY)
        self.assertEqual(res[0], ("hello",))

    async def test_select_varchar(self):
        await self._test_select_typ("SELECT 'hel''lo €'::VARCHAR", "hel'lo €")

    async def test_select_bpchar(self):
        await self._test_select_typ("SELECT 'hel''lo €'::CHAR(9)", "hel'lo € ")

    async def test_select_name(self):
        await self._test_select_typ("SELECT 'hel''lo €'::name", "hel'lo €")

    async def test_select_oid(self):
        await self._test_select_typ("SELECT 2147483648::oid", 2147483648)

    async def test_select_char(self):
        await self._test_select_typ("SELECT 'd'::\"char\"", "d")

    async def test_select_param(self):
        res = await self._cn.execute("SELECT $1, $2", 3, None)
        self.assertEqual(res[0], (3, None))

    async def test_select_wrong_number_params(self):
        with self.assertRaises(ServerError):
            await self._cn.execute("SELECT $1, $2", (1,))
        self.assertEqual(self._cn.status, ProtocolStatus.READY_FOR_QUERY)

    async def test_raw_result(self):
        res = await self._cn.execute("SELECT 123, 2000000000", raw_result=True)
        self.assertEqual(("123", "2000000000"), res[0])
        res = await self._cn.execute(
            "SELECT 123, 2000000000", result_format=Format.TEXT,
            raw_result=True)
        self.assertEqual(("123", "2000000000"), res[0])
        res = await self._cn.execute(
            "SELECT 123, 2000000000", result_format=Format.BINARY,
            raw_result=True)
        self.assertEqual(
            (struct.pack("!i", 123), struct.pack("!i", 2000000000)), res[0])

    async def asyncTearDown(self) -> None:
        self._cn.close()
