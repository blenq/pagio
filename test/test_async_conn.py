try:
    from unittest import IsolatedAsyncioTestCase
except ImportError:
    from later.unittest.backport.async_case import IsolatedAsyncioTestCase


from pagio import (
    AsyncConnection, TransactionStatus, ProtocolStatus, ServerError, Format)


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
