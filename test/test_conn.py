import unittest


from pagio import (
    Connection, TransactionStatus, ProtocolStatus, ServerError, Format,
    sync_connection, sync_protocol
)


class ConnCase(unittest.TestCase):

    def test_af_unix_conn(self):
        cn = Connection(database="postgres")
        self.assertIs(cn.transaction_status, TransactionStatus.IDLE)
        cn.close()

    def test_ip_conn(self):
        cn = Connection(
            host='localhost', database='postgres', password='owiCelm1')
        self.assertIs(cn.transaction_status, TransactionStatus.IDLE)
        cn.close()

    def test_simple_query(self):
        cn = Connection(database="postgres")
        res = cn.execute("SET TIMEZONE TO 'Europe/Paris'")
        self.assertEqual(res.command_tag, "SET")
        self.assertEqual(cn.tz_info.key, 'Europe/Paris')
        with self.assertRaises(ServerError) as ex_con:
            cn.execute("SET TIMEZONE TO 'Europe/Pariss'")
            print(ex_con)
        self.assertEqual(cn.status, ProtocolStatus.READY_FOR_QUERY)
        cn.close()

    def test_close(self):
        cn = Connection(database="postgres")
        cn.close()
        cn.close()
        self.assertEqual(cn.status, ProtocolStatus.CLOSED)

    def test_context(self):
        with Connection(database="postgres") as cn:
            self.assertEqual(cn.status, ProtocolStatus.READY_FOR_QUERY)
        self.assertEqual(cn.status, ProtocolStatus.CLOSED)

    def test_empty_query(self):
        with Connection(database="postgres") as cn:
            self.assertFalse(cn.execute(""))
            self.assertFalse(cn.execute("-- hi"))

    def test_extended_nodata(self):
        with Connection(database="postgres") as cn:
            res = cn.execute(
                "SET TIMEZONE TO 'Europe/Paris'", result_format=Format.BINARY)
            self.assertIsNone(res.fields)


class PyConnCase(ConnCase):
    @classmethod
    def setUpClass(cls) -> None:
        sync_connection._protocol_class = sync_protocol.PyPGProtocol

    @classmethod
    def tearDownClass(cls) -> None:
        sync_connection._protocol_class = sync_protocol.PGProtocol
