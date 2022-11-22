from io import BytesIO, StringIO
import unittest

try:
    from unittest import IsolatedAsyncioTestCase
except ImportError:
    from later.unittest.backport.async_case import IsolatedAsyncioTestCase

from pagio import (
    sync_connection, sync_protocol, Connection, async_connection,
    async_protocol, AsyncConnection, ServerError, Format, ProtocolStatus)


def get_lines(num):
    yield "id,strval,dateval,datetimeval,floatval\n"
    for i in range(num):
        yield f"{i},héllo_{i}_hi,2020-01-03,2020-01-03 14:32,{3.4 * i}\n"


create_temp_tbl_sql = """
    CREATE TEMPORARY TABLE input_table (
        id serial,
        strval text,
        dateval date,
        datetimeval timestamp,
        floatval float8);"""


def get_input_file(encoding='utf-8'):
    fake_file = BytesIO()
    for line in get_lines(1000):
        fake_file.write(line.encode(encoding))
    fake_file.seek(0)
    return fake_file


def get_input_text_file():
    fake_file = StringIO()
    for line in get_lines(1000):
        fake_file.write(line)
    fake_file.seek(0)
    return fake_file


class SpecialException(Exception):
    ...


class ErrorRaisingFile:
    def __init__(self, file_obj):
        self.file_obj = file_obj
        self.times = 0

    def read(self, num):
        if self.times == 3:
            raise SpecialException
        self.times += 1
        return self.file_obj.read(num)


class CopyCase(unittest.TestCase):

    def test_copy_into_db(self):
        with Connection(database="postgres") as cn:
            cn.execute(create_temp_tbl_sql)

            file_obj = get_input_file()
            res = cn.execute(
                "COPY input_table FROM STDIN (HEADER MATCH, FORMAT CSV)",
                file_obj=file_obj)
            self.assertEqual(1000, res.records_affected)
            res = cn.execute("SELECT COUNT(*) FROM input_table")
            self.assertEqual(1000, res.rows[0][0])

    def test_copy_text_into_db(self):
        with Connection(database="postgres") as cn:
            cn.execute(create_temp_tbl_sql)

            file_obj = get_input_text_file()
            res = cn.execute(
                "COPY input_table FROM STDIN (HEADER MATCH, FORMAT CSV)",
                file_obj=file_obj)
            self.assertEqual(1000, res.records_affected)
            res = cn.execute("SELECT COUNT(*) FROM input_table")
            self.assertEqual(1000, res.rows[0][0])

    def test_copy_into_db_extended(self):
        # use Binary format to force Extended Query Protocol
        with Connection(database="postgres") as cn:
            cn.execute(create_temp_tbl_sql)
            file_obj = get_input_file()
            res = cn.execute(
                "COPY input_table FROM STDIN (HEADER MATCH, FORMAT CSV)",
                file_obj=file_obj, result_format=Format.BINARY)
            self.assertEqual(1000, res.records_affected)
            res = cn.execute("SELECT COUNT(*) FROM input_table")
            self.assertEqual(1000, res.rows[0][0])

    def test_copy_into_latin1(self):
        with Connection(database="postgres") as cn:
            cn.execute(create_temp_tbl_sql)
            file_obj = get_input_file("latin-1")

            with self.assertRaises(ServerError):
                # Input file has wrong encoding
                cn.execute(
                    "COPY input_table FROM STDIN (HEADER MATCH, FORMAT CSV)",
                    file_obj=file_obj)

            # Try again and specify encoding in statement
            file_obj.seek(0)
            res = cn.execute(
                "COPY input_table FROM STDIN ("
                "   HEADER MATCH, FORMAT CSV, ENCODING LATIN1)",
                file_obj=file_obj)
            self.assertEqual(1000, res.records_affected)
            res = cn.execute("SELECT COUNT(*) FROM input_table")
            self.assertEqual(1000, res.rows[0][0])

    def test_copy_into_latin1_extended(self):
        # use binary format to force extended query
        with Connection(database="postgres") as cn:
            cn.execute(create_temp_tbl_sql)
            file_obj = get_input_file("latin-1")

            with self.assertRaises(ServerError):
                # Input file has wrong encoding
                cn.execute(
                    "COPY input_table FROM STDIN (HEADER MATCH, FORMAT CSV)",
                    file_obj=file_obj, result_format=Format.BINARY)

            # Try again and specify encoding in statement
            file_obj.seek(0)
            res = cn.execute(
                "COPY input_table FROM STDIN ("
                "   HEADER MATCH, FORMAT CSV, ENCODING LATIN1)",
                file_obj=file_obj, result_format=Format.BINARY)
            self.assertEqual(1000, res.records_affected)
            res = cn.execute("SELECT COUNT(*) FROM input_table")
            self.assertEqual(1000, res.rows[0][0])

    def test_error_raising_file(self):
        with Connection(database="postgres") as cn:
            cn.execute(create_temp_tbl_sql)
            file_obj = ErrorRaisingFile(get_input_file())
            with self.assertRaises(SpecialException):
                cn.execute(
                    "COPY input_table FROM STDIN (HEADER MATCH, FORMAT CSV)",
                    file_obj=file_obj)
            self.assertEqual(cn.status, ProtocolStatus.READY_FOR_QUERY)

    def test_error_raising_file_extended(self):
        with Connection(database="postgres") as cn:
            cn.execute(create_temp_tbl_sql)
            file_obj = ErrorRaisingFile(get_input_file())
            with self.assertRaises(SpecialException):
                cn.execute(
                    "COPY input_table FROM STDIN (HEADER MATCH, FORMAT CSV)",
                    file_obj=file_obj, result_format=Format.BINARY)
            self.assertEqual(cn.status, ProtocolStatus.READY_FOR_QUERY)


class PyCopyCase(CopyCase):
    @classmethod
    def setUpClass(cls) -> None:
        # Monkey patch to force pure Python PGProtocol
        sync_connection.PGProtocol = sync_protocol.PyPGProtocol

    @classmethod
    def tearDownClass(cls) -> None:
        # Reset monkey patch
        sync_connection.PGProtocol = sync_protocol.PGProtocol


class AsyncCopyCase(IsolatedAsyncioTestCase):

    async def test_copy_into_db(self):
        with await AsyncConnection(database="postgres") as cn:
            await cn.execute(create_temp_tbl_sql)
            file_obj = get_input_file()
            res = await cn.execute(
                "COPY input_table FROM STDIN (HEADER MATCH, FORMAT CSV)",
                file_obj=file_obj)
            self.assertEqual(1000, res.records_affected)
            res = await cn.execute("SELECT COUNT(*) FROM input_table")
            self.assertEqual(1000, res.rows[0][0])

    async def test_copy_text_into_db(self):
        with await AsyncConnection(database="postgres") as cn:
            await cn.execute(create_temp_tbl_sql)

            file_obj = get_input_text_file()
            res = await cn.execute(
                "COPY input_table FROM STDIN (HEADER MATCH, FORMAT CSV)",
                file_obj=file_obj)
            self.assertEqual(1000, res.records_affected)
            res = await cn.execute("SELECT COUNT(*) FROM input_table")
            self.assertEqual(1000, res.rows[0][0])

    async def test_copy_into_db_extended(self):
        # use Binary format to force Extended Query Protocol
        with await AsyncConnection(database="postgres") as cn:
            await cn.execute(create_temp_tbl_sql)
            file_obj = get_input_file()
            res = await cn.execute(
                "COPY input_table FROM STDIN (HEADER MATCH, FORMAT CSV)",
                file_obj=file_obj, result_format=Format.BINARY)
            self.assertEqual(1000, res.records_affected)
            res = await cn.execute("SELECT COUNT(*) FROM input_table")
            self.assertEqual(1000, res.rows[0][0])

    async def test_copy_into_latin1(self):
        with await AsyncConnection(database="postgres") as cn:
            await cn.execute(create_temp_tbl_sql)
            file_obj = get_input_file("latin-1")

            with self.assertRaises(ServerError):
                # Input file has wrong encoding
                await cn.execute(
                    "COPY input_table FROM STDIN (HEADER MATCH, FORMAT CSV)",
                    file_obj=file_obj)

            # Try again and specify encoding in statement
            file_obj = get_input_file("latin-1")
            res = await cn.execute(
                "COPY input_table FROM STDIN ("
                "   HEADER MATCH, FORMAT CSV, ENCODING LATIN1)",
                file_obj=file_obj)
            self.assertEqual(1000, res.records_affected)
            res = await cn.execute("SELECT COUNT(*) FROM input_table")
            self.assertEqual(1000, res.rows[0][0])

    async def test_copy_into_latin1_extended(self):
        # use binary format to force extended query
        with await AsyncConnection(database="postgres") as cn:
            await cn.execute(create_temp_tbl_sql)
            file_obj = get_input_file("latin-1")

            with self.assertRaises(ServerError):
                # Input file has wrong encoding
                await cn.execute(
                    "COPY input_table FROM STDIN (HEADER MATCH, FORMAT CSV)",
                    file_obj=file_obj, result_format=Format.BINARY)

            # Try again and specify encoding in statement
            file_obj = get_input_file("latin-1")
            res = await cn.execute(
                "COPY input_table FROM STDIN ("
                "   HEADER MATCH, FORMAT CSV, ENCODING LATIN1)",
                file_obj=file_obj, result_format=Format.BINARY)
            self.assertEqual(1000, res.records_affected)
            res = await cn.execute("SELECT COUNT(*) FROM input_table")
            self.assertEqual(1000, res.rows[0][0])

    async def test_error_raising_file(self):
        with await AsyncConnection(database="postgres") as cn:
            await cn.execute(create_temp_tbl_sql)
            file_obj = ErrorRaisingFile(get_input_file())
            with self.assertRaises(SpecialException):
                await cn.execute(
                    "COPY input_table FROM STDIN (HEADER MATCH, FORMAT CSV)",
                    file_obj=file_obj)
            self.assertEqual(cn.status, ProtocolStatus.READY_FOR_QUERY)

    async def test_error_raising_file_extended(self):
        with await AsyncConnection(database="postgres") as cn:
            await cn.execute(create_temp_tbl_sql)
            file_obj = ErrorRaisingFile(get_input_file())
            with self.assertRaises(SpecialException):
                await cn.execute(
                    "COPY input_table FROM STDIN (HEADER MATCH, FORMAT CSV)",
                    file_obj=file_obj, result_format=Format.BINARY)
            self.assertEqual(cn.status, ProtocolStatus.READY_FOR_QUERY)


class PyAsyncCopyCase(AsyncCopyCase):

    @classmethod
    def setUpClass(cls) -> None:
        async_connection.AsyncPGProtocol = async_protocol.PyAsyncPGProtocol

    @classmethod
    def tearDownClass(cls) -> None:
        async_connection.AsyncPGProtocol = async_protocol.AsyncPGProtocol
