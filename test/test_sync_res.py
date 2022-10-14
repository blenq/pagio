import unittest

from pagio import (
    Connection, Format, ServerError, ProtocolStatus, sync_connection,
    sync_protocol)


class ResultCase(unittest.TestCase):

    def setUp(self) -> None:
        self._cn = Connection(database="postgres")

    def test_select(self):
        res = self._cn.execute("SELECT '12'")
        self.assertEqual(res[0][0], '12')

    def test_large_buffer(self):
        res = self._cn.execute("SELECT rpad('', 32000, '0');")
        self.assertEqual(res[0][0], '0' * 32000)

    def test_select_iter(self):
        res = self._cn.execute("SELECT '12' UNION SELECT '13' ORDER BY 1")
        i = 12
        for row in res:
            self.assertIsInstance(row, tuple)
            self.assertEqual(row[0], str(i))
            i += 1

    def test_select_none_param(self):
        res = self._cn.execute("SELECT $1", None)
        self.assertEqual(res[0], (None,))
        self.assertEqual(res.fields[0].type_oid, 25)

    def test_select_int(self):
        res = self._cn.execute("SELECT 12, 12::int4, 12::int8, 12::int2")
        self.assertEqual(res[0], (12, 12, 12, 12))
        res = self._cn.execute(
            "SELECT 12, 12::int4, 12::int8, 12::int2",
            result_format=Format.BINARY)
        self.assertEqual(res[0], (12, 12, 12, 12))

    def test_select_float(self):
        res = self._cn.execute("SELECT 1.5::float4, 1.5::float8")
        self.assertEqual(res[0], (1.5, 1.5))
        res = self._cn.execute(
            "SELECT 1.5::float4, 1.5::float8", result_format=Format.BINARY)
        self.assertEqual(res[0], (1.5, 1.5))

    def test_select_float_param(self):
        res = self._cn.execute("SELECT $1", 1.5)
        self.assertEqual(res[0], (1.5,))

    def test_select_bool(self):
        res = self._cn.execute("SELECT true, false")
        self.assertEqual(res[0], (True, False))
        res = self._cn.execute(
            "SELECT true, false", result_format=Format.BINARY)
        self.assertEqual(res[0], (True, False))

    def test_select_bool_param(self):
        params = (True, False)
        res = self._cn.execute("SELECT $1, $2", True, False)
        self.assertEqual(res[0], params)

    def _test_select_typ(self, sql, val):
        res = self._cn.execute(sql)
        self.assertEqual(res[0], (val,))
        res = self._cn.execute(sql, result_format=Format.BINARY)
        self.assertEqual(res[0], (val,))

    def test_select_text(self):
        self._test_select_typ("SELECT 'hel''lo €'", "hel'lo €")

    def test_select_string_param(self):
        res = self._cn.execute("SELECT $1", "hello")
        self.assertEqual(res[0], ("hello",))
        res = self._cn.execute("SELECT $1 || 'a'", "hello")
        self.assertEqual(res[0], ("helloa",))
        res = self._cn.execute(
            "SELECT $1", "hello", result_format=Format.BINARY)
        self.assertEqual(res[0], ("hello",))

    def test_select_varchar(self):
        self._test_select_typ("SELECT 'hel''lo €'::VARCHAR", "hel'lo €")

    def test_select_bpchar(self):
        self._test_select_typ("SELECT 'hel''lo €'::CHAR(9)", "hel'lo € ")

    def test_select_name(self):
        self._test_select_typ("SELECT 'hel''lo €'::name", "hel'lo €")

    def test_select_oid(self):
        self._test_select_typ("SELECT 2147483648::oid", 2147483648)

    def test_select_char(self):
        self._test_select_typ("SELECT 'd'::\"char\"", "d")

    def test_select_param(self):
        res = self._cn.execute("SELECT $1, $2", 3, None)
        self.assertEqual(res[0], (3, None))

    def test_select_wrong_number_params(self):
        with self.assertRaises(ServerError):
            self._cn.execute("SELECT $1, $2", (1,))
        self.assertEqual(self._cn.status, ProtocolStatus.READY_FOR_QUERY)

    def tearDown(self) -> None:
        self._cn.close()


class PyResultCase(ResultCase):
    @classmethod
    def setUpClass(cls) -> None:
        sync_connection._protocol_class = sync_protocol.PyPGProtocol

    @classmethod
    def tearDownClass(cls) -> None:
        sync_connection._protocol_class = sync_protocol.PGProtocol