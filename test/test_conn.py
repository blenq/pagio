import gc
import unittest
import weakref

from pagio import (
    Connection, TransactionStatus, ProtocolStatus, ServerError, Format,
    sync_connection, sync_protocol, CachedQueryExpired, StatementDoesNotExist,
    SSLMode,
)


class ConnCase(unittest.TestCase):

    def test_af_unix_conn(self):
        cn = Connection(database="postgres")
        self.assertIs(cn.transaction_status, TransactionStatus.IDLE)
        cn.close()

    def test_ip_conn(self):
        cn = Connection(
            host='localhost', database='postgres', password='hoi\uE100')
        self.assertIs(cn.transaction_status, TransactionStatus.IDLE)
        cn.close()

    def test_ip_conn_no_ssl(self):
        cn = Connection(
            host='localhost', database='postgres', password='hoi\uE100',
            ssl_mode=SSLMode.DISABLE)
        self.assertIs(cn.transaction_status, TransactionStatus.IDLE)
        cn.close()

    def test_parameter_status(self):
        cn = Connection(database="postgres")
        self.assertEqual("UTF8", cn.server_parameters["client_encoding"])
        cn.close()

    def test_simple_query(self):
        cn = Connection(database="postgres")
        res = cn.execute("SET TIMEZONE TO 'Europe/Paris'")
        self.assertEqual(res.command_tag, "SET")
        self.assertEqual(cn.tzinfo.key, 'Europe/Paris')
        with self.assertRaises(ServerError):
            cn.execute("SET TIMEZONE TO 'Europe/Pariss'")
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


    def test_select_cache(self):
        sql1 = "SELECT $1, $2"
        sql2 = "SELECT $1 + 1, $2 + 1, $3"
        with Connection(database="postgres", prepare_threshold=4) as cn:
            # execute up to threshold
            for i in range(4):
                res = cn.execute(sql1, i, None)
                self.assertEqual(res.rows[0], (i, None))
                res = cn.execute(sql2, i, None, 'hi')
                self.assertEqual(res.rows[0], (i + 1, None, 'hi'))

            # assert not prepared yet
            res = cn.execute(
                "SELECT COUNT(*) FROM pg_prepared_statements "
                "WHERE statement in ($1, $2)", sql1, sql2)
            self.assertEqual(res[0][0], 0)

            # execute both queries one more time
            res = cn.execute(sql1, i, None)
            self.assertEqual(res.rows[0], (i, None))
            res = cn.execute(sql2, i, None, 'hi')
            self.assertEqual(res.rows[0], (i + 1, None, 'hi'))

            # assert prepared
            res = cn.execute(
                "SELECT COUNT(*) FROM pg_prepared_statements "
                "WHERE statement in ($1, $2)", sql1, sql2)
            self.assertEqual(res[0][0], 2)

            # execute both queries and assert those still work
            res = cn.execute(sql1, i, None)
            self.assertEqual(res.rows[0], (i, None))
            res = cn.execute(sql2, i, None, 'hi')
            self.assertEqual(res.rows[0], (i + 1, None, 'hi'))

    def test_max_cache(self):
        with Connection(
               database="postgres", prepare_threshold=1, cache_size=10) as cn:
            cn.execute("SELECT 1 AS val")
            cn.execute("SELECT 1 AS val")

            # executed twice, so should be prepared now
            res = cn.execute("SELECT COUNT(*) FROM pg_prepared_statements")
            self.assertEqual(res.rows[0][0], 1)

            # move statement out of cache by executing other statements
            for i in range(9):
                cn.execute(f"SELECT {i}")

            # verify it is not prepared anymore
            res = cn.execute(
                "SELECT COUNT(*) AS num FROM pg_prepared_statements")
            self.assertEqual(res.rows[0][0], 0)

            cn.execute("SELECT 1 AS val")
            cn.execute("SELECT 1 AS val")

            # executed twice, so should be prepared now
            res = cn.execute(
                "SELECT COUNT(*) AS num2 FROM pg_prepared_statements")
            self.assertEqual(res.rows[0][0], 1)

            # cn.execute("PREPARE myplan AS SELECT 2 AS val;")
            # res = cn.execute("SELECT COUNT(*) AS num3 FROM pg_prepared_statements")
            # self.assertEqual(res.rows[0][0], 2)
            # cn.execute("DISCARD ALL").command_tag
            # cn.execute("SELECT 1 AS val")
            # cn.execute("SELECT 1 AS val")
            # cn.execute("DEALLOCATE ALL").command_tag
            # cn.execute("SELECT 1 AS val")

    def test_cache_error(self):
        with Connection(
               database="postgres", prepare_threshold=1) as cn:
            res = cn.execute("SELECT 12 / $1 AS val", 2)
            self.assertEqual(res.rows[0], (6,))
            res = cn.execute("SELECT 12 / $1 AS val", 3)
            self.assertEqual(res.rows[0], (4,))

            # executed twice, so should be prepared now
            res = cn.execute("SELECT COUNT(*) FROM pg_prepared_statements")
            self.assertEqual(res.rows[0][0], 1)

            with self.assertRaises(ServerError):
                cn.execute("SELECT 12 / $1 AS val", 0)

            res = cn.execute("SELECT 12 / $1 AS val", 4)
            self.assertEqual(res.rows[0], (3,))

            # error occurred, so should be cleared
            res = cn.execute(
                "SELECT COUNT(*) as num1 FROM pg_prepared_statements")
            self.assertEqual(res.rows[0][0], 0)

            res = cn.execute("SELECT 12 / $1 AS val", 3)
            self.assertEqual(res.rows[0], (4,))

            # executed twice, so should be prepared now
            res = cn.execute(
                "SELECT COUNT(*) AS num2 FROM pg_prepared_statements")
            self.assertEqual(res.rows[0][0], 1)

    def test_multi_stmt(self):
        with Connection(database="postgres") as cn:
            res = cn.execute("BEGIN;SELECT 1;COMMIT")
            self.assertIsNone(res.fields)
            res.next_result()
            self.assertIsNotNone(res.fields)
            res.next_result()
            self.assertIsNone(res.fields)

    def test_expired_statement(self):
        with Connection(database="postgres", prepare_threshold=1) as cn:
            cn.execute("CREATE TEMPORARY TABLE test_val (id serial, val int)")
            cn.execute("SELECT * FROM test_val")

            # execute for the second time, will prepare the statement
            cn.execute("SELECT * FROM test_val")

            # recreate table with different type
            cn.execute(
                "DROP TABLE test_val;"
                "CREATE TEMPORARY TABLE test_val (id serial, val text)")

            # cached statement is not valid anymore, connection should recover
            # from error
            res = cn.execute("SELECT * FROM test_val")
            self.assertEqual(res.fields[1].type_oid, 25)

            # do the same within transaction. Connection can not recover now
            cn.execute("BEGIN;")
            cn.execute("SELECT * FROM test_val")
            # recreate table with different type
            cn.execute(
                "DROP TABLE test_val;"
                "CREATE TEMPORARY TABLE test_val (id serial, val int)")
            with self.assertRaises(CachedQueryExpired):
                res = cn.execute("SELECT * FROM test_val")
            cn.execute("ROLLBACK")

    def test_cache_change_format(self):
        with Connection(database="postgres", prepare_threshold=1) as cn:
            cn.execute("SELECT 16")
            cn.execute("SELECT 16")
            res = cn.execute("SELECT 16", result_format=Format.TEXT)
            self.assertEqual(16, res.rows[0][0])
            res = cn.execute("SELECT 16", result_format=Format.BINARY)
            self.assertEqual(16, res.rows[0][0])

    def test_discard_all(self):
        with Connection(database="postgres", prepare_threshold=1) as cn:
            cn.execute("SELECT 1")
            cn.execute("SELECT 1")
            cn.execute("DISCARD ALL")
            cn.execute("SELECT 1")
            cn.execute("SELECT 1")
            cn.execute("DEALLOCATE ALL")
            res = cn.execute("SELECT 1")
            self.assertEqual(1, res.rows[0][0])

    def test_discard_one(self):
        with Connection(database="postgres", prepare_threshold=1) as cn:
            cn.execute("SELECT 1")
            cn.execute("SELECT 1")

            # statement is prepared now, deallocate to confuse cache
            res = cn.execute(
                "SELECT name FROM pg_prepared_statements "
                "WHERE statement = 'SELECT 1';")
            cn.execute("DEALLOCATE " + res.rows[0][0])

            # should recover from error and successfully execute
            res = cn.execute("SELECT 1")
            self.assertEqual(1, res.rows[0][0])

            # now do the same in a transaction
            cn.execute("SELECT 1")
            cn.execute("BEGIN")
            res = cn.execute(
                "SELECT name FROM pg_prepared_statements "
                "WHERE statement = 'SELECT 1'")
            cn.execute("DEALLOCATE  " + res.rows[0][0])
            with self.assertRaises(StatementDoesNotExist):
                # Pagio can't recover in a transaction
                cn.execute("SELECT 1")

    def test_cache_with_trans(self):
        # See if statement caching works in combination with transactions.
        with Connection(database="postgres", prepare_threshold=1) as cn:
            cn.execute("BEGIN")
            cn.execute("SELECT 1")
            cn.execute("SELECT 1")
            cn.execute("ROLLBACK")
            res = cn.execute(
                "SELECT COUNT(*) as num1 FROM pg_prepared_statements")
            self.assertEqual(res.rows[0][0], 1)
            res = cn.execute("SELECT 1")
            self.assertEqual(1, res.rows[0][0])

    def test_weakrefs(self):
        with Connection(database="postgres", prepare_threshold=1) as cn:
            cn.execute("SELECT 1")
            wr = weakref.ref(cn)
            gc.collect()
            self.assertIs(cn, wr())
        del cn
        gc.collect()
        self.assertIsNone(wr())


class PyConnCase(ConnCase):
    @classmethod
    def setUpClass(cls) -> None:
        sync_connection.PGProtocol = sync_protocol.PyPGProtocol

    @classmethod
    def tearDownClass(cls) -> None:
        sync_connection.PGProtocol = sync_protocol.PGProtocol
