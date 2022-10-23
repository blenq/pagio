try:
    from unittest import IsolatedAsyncioTestCase
except ImportError:
    from later.unittest.backport.async_case import IsolatedAsyncioTestCase


from pagio import (
    AsyncConnection, TransactionStatus, ProtocolStatus, ServerError, Format,
    async_connection, async_protocol, CachedQueryExpired)


class ConnCase(IsolatedAsyncioTestCase):

    async def test_af_unix_conn(self):
        cn = await AsyncConnection(database="postgres")
        self.assertIs(cn.transaction_status, TransactionStatus.IDLE)
        await cn.close()

    async def test_not_awaited_closed(self):
        cn = AsyncConnection(database="postgress")
        self.assertEqual(cn.status, ProtocolStatus.CLOSED)

    async def test_wrong_db(self):
        cn = AsyncConnection(database="postgress")
        try:
            await cn
        except:
            pass
        self.assertEqual(cn.status, ProtocolStatus.CLOSED)

    async def test_ip_conn(self):
        cn = await AsyncConnection(
            host='localhost', database='postgres', password='owiCelm1')
        self.assertIs(cn.transaction_status, TransactionStatus.IDLE)
        await cn.close()

    async def test_simple_query(self):
        cn = await AsyncConnection(database="postgres")
        res = await cn.execute("SET TIMEZONE TO 'Europe/Paris'")
        self.assertEqual(res.command_tag, "SET")
        self.assertEqual(cn.tz_info.key, 'Europe/Paris')
        with self.assertRaises(ServerError):
            await cn.execute("SET TIMEZONE TO 'Europe/Pariss'")
        self.assertEqual(cn.status, ProtocolStatus.READY_FOR_QUERY)
        await cn.close()

    async def test_close(self):
        cn = await AsyncConnection(database="postgres")
        await cn.close()
        await cn.close()

    async def test_context(self):
        async with await AsyncConnection(database="postgres") as cn:
            self.assertEqual(cn.status, ProtocolStatus.READY_FOR_QUERY)
        self.assertEqual(cn.status, ProtocolStatus.CLOSED)

    async def test_empty_query(self):
        async with await AsyncConnection(database="postgres") as cn:
            res = await cn.execute("")
            self.assertFalse(await cn.execute(""))
            self.assertFalse(await cn.execute("-- hi"))

    async def test_extended_nodata(self):
        async with await AsyncConnection(database="postgres") as cn:
            res = await cn.execute(
                "SET TIMEZONE TO 'Europe/Paris'", result_format=Format.BINARY)
            self.assertIsNone(res.fields)

    async def test_select_cache(self):
        sql1 = "SELECT $1, $2"
        sql2 = "SELECT $1 + 1, $2 + 1, $3"
        async with await AsyncConnection(
                database="postgres", prepare_threshold=4) as cn:
            # execute up to threshold
            for i in range(4):
                res = await cn.execute(sql1, i, None)
                self.assertEqual(res.rows[0], (i, None))
                res = await cn.execute(sql2, i, None, 'hi')
                self.assertEqual(res.rows[0], (i + 1, None, 'hi'))

            # assert not prepared yet
            res = await cn.execute(
                "SELECT COUNT(*) FROM pg_prepared_statements "
                "WHERE statement in ($1, $2)", sql1, sql2)
            self.assertEqual(res[0][0], 0)

            # execute both queries one more time
            res = await cn.execute(sql1, i, None)
            self.assertEqual(res.rows[0], (i, None))
            res = await cn.execute(sql2, i, None, 'hi')
            self.assertEqual(res.rows[0], (i + 1, None, 'hi'))

            # assert prepared
            res = await cn.execute(
                "SELECT COUNT(*) FROM pg_prepared_statements "
                "WHERE statement in ($1, $2)", sql1, sql2)
            self.assertEqual(res[0][0], 2)

            # execute both queries and assert those still work
            res = await cn.execute(sql1, i, None)
            self.assertEqual(res.rows[0], (i, None))
            res = await cn.execute(sql2, i, None, 'hi')
            self.assertEqual(res.rows[0], (i + 1, None, 'hi'))

    async def test_max_cache(self):
       async with await AsyncConnection(
               database="postgres", prepare_threshold=1, cache_size=10
       ) as cn:
           await cn.execute("SELECT 1 AS val")
           await cn.execute("SELECT 1 AS val")
           res = await cn.execute("SELECT COUNT(*) FROM pg_prepared_statements")
           self.assertEqual(res.rows[0][0], 1)
           for i in range(10):
               await cn.execute(f"SELECT {i}")
           res = await cn.execute("SELECT COUNT(*) FROM pg_prepared_statements")
           self.assertEqual(res.rows[0][0], 0)

    async def test_cache_error(self):
        async with await AsyncConnection(
               database="postgres", prepare_threshold=1) as cn:
            res = await cn.execute("SELECT 12 / $1 AS val", 2)
            self.assertEqual(res.rows[0], (6,))
            res = await cn.execute("SELECT 12 / $1 AS val", 3)
            self.assertEqual(res.rows[0], (4,))

            # executed twice, so should be prepared now
            res = await cn.execute("SELECT COUNT(*) FROM pg_prepared_statements")
            self.assertEqual(res.rows[0][0], 1)

            with self.assertRaises(ServerError):
                await cn.execute("SELECT 12 / $1 AS val", 0)

            res = await cn.execute("SELECT 12 / $1 AS val", 4)
            self.assertEqual(res.rows[0], (3,))

            # error occurred, so should be cleared
            res = await cn.execute(
                "SELECT COUNT(*) as num1 FROM pg_prepared_statements")
            self.assertEqual(res.rows[0][0], 0)

            res = await cn.execute("SELECT 12 / $1 AS val", 3)
            self.assertEqual(res.rows[0], (4,))

            # executed twice, so should be prepared now
            res = await cn.execute(
                "SELECT COUNT(*) AS num2 FROM pg_prepared_statements")
            self.assertEqual(res.rows[0][0], 1)

    async def test_multi_stmt(self):
        async with await AsyncConnection(database="postgres") as cn:
            res = await cn.execute("BEGIN;SELECT 1;COMMIT")
            self.assertIsNone(res.fields)
            res.next_result()
            self.assertIsNotNone(res.fields)
            res.next_result()
            self.assertIsNone(res.fields)

    async def test_expired_statement(self):
        async with await AsyncConnection(
                database="postgres", prepare_threshold=1) as cn:
            await cn.execute("CREATE TEMPORARY TABLE test_val (id serial, val int)")
            await cn.execute("SELECT * FROM test_val")

            # execute for the second time, will prepare the statement
            await cn.execute("SELECT * FROM test_val")

            # recreate table with different type
            await cn.execute(
                "DROP TABLE test_val;"
                "CREATE TEMPORARY TABLE test_val (id serial, val text)")

            # cached statement is not valid anymore, connection should recover
            res = await cn.execute("SELECT * FROM test_val")
            self.assertEqual(res.fields[1].type_oid, 25)

            # do the same within transaction. Connection can not recover now
            await cn.execute("BEGIN;")
            await cn.execute("SELECT * FROM test_val")
            # recreate table with different type
            await cn.execute(
                "DROP TABLE test_val;"
                "CREATE TEMPORARY TABLE test_val (id serial, val int)")
            with self.assertRaises(CachedQueryExpired):
                res = await cn.execute("SELECT * FROM test_val")
            await cn.execute("ROLLBACK")

class PyConnCase(ConnCase):
    @classmethod
    def setUpClass(cls) -> None:
        async_connection.AsyncPGProtocol = async_protocol.PyAsyncPGProtocol

    @classmethod
    def tearDownClass(cls) -> None:
        async_connection.AsyncPGProtocol = async_protocol.AsyncPGProtocol
