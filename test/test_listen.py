import asyncio

try:
    from unittest import IsolatedAsyncioTestCase
except ImportError:
    from later.unittest.backport.async_case import IsolatedAsyncioTestCase

from pagio import async_connection, async_protocol, AsyncConnection


class AsyncListenCase(IsolatedAsyncioTestCase):

    async def test_basic_listen(self):
        async with await AsyncConnection(database="postgres") as cn:
            await cn.execute("LISTEN chan")
            await cn.execute("NOTIFY chan, 'yes'")
            notification = await cn.notifications.get()
            self.assertEqual(notification.payload, "yes")
            await cn.execute("NOTIFY chan")
            notification = await cn.notifications.get()
            self.assertEqual(notification.payload, "")

    async def test_basic_listen_multi_process(self):

        async with await AsyncConnection(database="postgres") as cn1:
            async with await AsyncConnection(database="postgres") as cn2:
                await cn1.execute("LISTEN chan")
                task = asyncio.create_task(cn1.notifications.get())
                await cn2.execute("NOTIFY chan, 'payload'")
                await task
                self.assertEqual(task.result().payload, "payload")


class PyAsyncListenCase(AsyncListenCase):

    @classmethod
    def setUpClass(cls) -> None:
        async_connection.AsyncPGProtocol = async_protocol.PyAsyncPGProtocol

    @classmethod
    def tearDownClass(cls) -> None:
        async_connection.AsyncPGProtocol = async_protocol.AsyncPGProtocol
