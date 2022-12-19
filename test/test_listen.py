import asyncio
from threading import Thread
import unittest

try:
    from unittest import IsolatedAsyncioTestCase
except ImportError:
    from later.unittest.backport.async_case import IsolatedAsyncioTestCase

from pagio import (
    async_connection, async_protocol, AsyncConnection, sync_connection,
    sync_protocol, Connection, QueueEmpty)


class ListenCase(unittest.TestCase):

    def test_basic_listen(self):
        with Connection(database="postgres") as cn:
            cn.execute("LISTEN chan")
            cn.execute("NOTIFY chan, 'yes'")
            cn.execute("NOTIFY chan")
            notification = cn.notifications.get()
            self.assertEqual(notification.payload, "yes")
            notification = cn.notifications.get()
            self.assertEqual(notification.payload, "")

    def test_multi_listen(self):
        notification = None

        def read_notification(cn):
            nonlocal notification
            notification = cn.notifications.get()

        with Connection(database="postgres") as cn1:
            with Connection(database="postgres") as cn2:
                cn1.execute("LISTEN chan")
                t = Thread(target=read_notification, args=(cn1,))
                t.start()
                cn2.execute("SELECT pg_sleep(0.1); NOTIFY chan, 'payload'")
                t.join()
                self.assertEqual(notification.payload, "payload")

    def test_timeout_listen(self):
        with Connection(database="postgres") as cn:
            with self.assertRaises(QueueEmpty):
                cn.notifications.get_nowait()
            with self.assertRaises(QueueEmpty):
                cn.notifications.get(0.1)


class PyListenCase(ListenCase):
    @classmethod
    def setUpClass(cls) -> None:
        # Monkey patch to force pure Python PGProtocol
        sync_connection.PGProtocol = sync_protocol.PyPGProtocol

    @classmethod
    def tearDownClass(cls) -> None:
        # Reset monkey patch
        sync_connection.PGProtocol = sync_protocol.PGProtocol


class AsyncListenCase(IsolatedAsyncioTestCase):

    async def test_basic_listen(self):
        async with await AsyncConnection(database="postgres") as cn:
            await cn.execute("LISTEN chan")
            await cn.execute("NOTIFY chan, 'yes'")
            await cn.execute("NOTIFY chan")
            notification = await cn.notifications.get()
            self.assertEqual(notification.payload, "yes")
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
