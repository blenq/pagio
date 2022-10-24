from abc import abstractmethod, ABC
from codecs import decode
from collections import OrderedDict
import enum
from hashlib import md5
from struct import Struct, unpack_from, pack
from typing import (
    Optional, Union, Dict, Callable, List, Any, Tuple, cast, Generator,
    Type, OrderedDict as TypingOrderedDict)

from .common import (
    ProtocolError, Severity, _error_fields, ServerError, InvalidOperationError,
    FieldInfo, CachedQueryExpired
)
from .const import *
from .zoneinfo import ZoneInfo, ZoneInfoNotFoundError

STANDARD_BUF_SIZE = 0x4000


header_struct_unpack_from = Struct("!Bi").unpack_from
ushort_struct_unpack_from = Struct('!H').unpack_from
int2_struct = Struct('!h')
int2_struct_unpack = int2_struct.unpack
int_struct = Struct('!i')
int_struct_unpack = int_struct.unpack
int_struct_unpack_from = int_struct.unpack_from

uint_struct = Struct('!I')
uint_struct_unpack = uint_struct.unpack

int8_struct = Struct('!q')
int8_struct_unpack = int8_struct.unpack

float4_struct = Struct('!f')
float4_struct_unpack = float4_struct.unpack
float8_struct = Struct('!d')
float8_struct_unpack = float8_struct.unpack

field_desc_struct = Struct(f"!IhIhih")
field_desc_struct_size = field_desc_struct.size
field_desc_struct_unpack_from = field_desc_struct.unpack_from

intint_struct = Struct('!ii')

single_byte_struct_unpack = Struct('!B').unpack


def check_length_equal(length: int, msg_buf: memoryview) -> None:
    if len(msg_buf) != length:
        raise ProtocolError(
            f"Invalid length for message. Expected {length}, but got"
            f"{len(msg_buf)}.")


_STATUS_CLOSED = 0
_STATUS_CLOSING = 1
_STATUS_CONNECTED = 2
_STATUS_SSL_REQUESTED = 3
_STATUS_STARTING_UP = 4
_STATUS_READY_FOR_QUERY = 5
_STATUS_EXECUTING = 6


class ProtocolStatus(enum.IntEnum):
    CLOSED = _STATUS_CLOSED
    CLOSING = _STATUS_CLOSING
    CONNECTED = _STATUS_CONNECTED
    SSL_REQUESTED = _STATUS_SSL_REQUESTED
    STARTING_UP = _STATUS_STARTING_UP
    READY_FOR_QUERY = _STATUS_READY_FOR_QUERY
    EXECUTING = _STATUS_EXECUTING


class TransactionStatus(enum.Enum):
    UNKNOWN = 0
    IDLE = ord('I')
    TRANSACTION = ord('T')
    ERROR = ord('E')


class Format(enum.IntEnum):
    DEFAULT = -1
    TEXT = 0
    BINARY = 1


_default_converters = [decode, bytes]


def none_to_pg(val: None, oid: int) -> Tuple[int, str, None, int, Format]:
    return oid, "", None, -1, Format.TEXT


def str_to_pg(val: Any, oid: int) -> Tuple[int, str, bytes, int, Format]:
    bytes_val = val.encode()
    val_len = len(bytes_val)
    return 0, f"{val_len}s", bytes_val, val_len, Format.TEXT


def default_to_pg(val: Any, oid: int) -> Tuple[int, str, bytes, int, Format]:
    return str_to_pg(str(val), oid)


def int_to_pg(val: int, oid: int) -> Tuple[int, str, Union[int, bytes], int, Format]:
    if -0x10000000 <= val <= 0x7FFFFFFF and oid != INT8OID:
        return INT4OID, "i", val, 4, Format.BINARY
    if -0x1000000000000000 <= val <= 0x7FFFFFFFFFFFFFFF:
        return INT8OID, "q", val, 8, Format.BINARY
    return default_to_pg(val, oid)


def float_to_pg(val: float, oid: int) -> Tuple[int, str, float, int, Format]:
    return FLOAT8OID, "d", val, 8, Format.BINARY


def bool_to_pg(val: bool, oid: int) -> Tuple[int, str, bool, int, Format]:
    return BOOLOID, "B", val, 1, Format.BINARY


def bin_int2_to_python(buf: memoryview) -> Any:
    return int2_struct_unpack(buf)[0]


def bin_int_to_python(buf: memoryview) -> Any:
    return int_struct_unpack(buf)[0]


def bin_uint_to_python(buf: memoryview) -> Any:
    return uint_struct_unpack(buf)[0]


def bin_int8_to_python(buf: memoryview) -> Any:
    return int8_struct_unpack(buf)[0]


def bin_float4_to_python(buf: memoryview) -> Any:
    return float4_struct_unpack(buf)[0]


def bin_float8_to_python(buf: memoryview) -> Any:
    return float8_struct_unpack(buf)[0]


def bin_bool_to_python(buf: memoryview) -> bool:
    if buf == b'\x01':
        return True
    if buf == b'\0':
        return False
    raise ProtocolError("Invalid value for bool")


def text_bool_to_python(buf: memoryview) -> bool:
    if buf == b't':
        return True
    if buf == b'f':
        return False
    raise ProtocolError("Invalid value for bool")


DBConverter = Callable[[memoryview], Any]


class _AbstractPGProtocol(ABC):
    @abstractmethod
    def _set_result(self, result: Any, final: bool) -> None:
        ...

    @abstractmethod
    def _set_exception(self, ex: BaseException) -> None:
        ...

    @abstractmethod
    def _close(self) -> None:
        ...

    @abstractmethod
    def get_buffer(self, sizehint: int) -> memoryview:
        ...

    @abstractmethod
    def buffer_updated(self, nbytes: int) -> None:
        ...

    @abstractmethod
    def execute_message(
        self,
        sql: str,
        parameters: Tuple[Any, ...],
        result_format: Format,
    ) -> List[bytes]:
        ...


class _BasePGProtocol(_AbstractPGProtocol):
    """ Common functionality for pure python and c accelerated versions of
    the PG protocol class, sync and async.

    """
    res_converters: Optional[List[DBConverter]]
    _transaction_status: int
    _ex: Optional[ServerError]

    def __init__(self) -> None:
        self._handlers: Dict[int, Callable[[memoryview], None]] = {
            ord(k): v for k, v in [
                # ' ': self.handle_ssl_response,
                ('E', self.handle_error),
                ('R', self.handle_auth_req),
                ('S', self.handle_parameter_status),
                ('K', self.handle_backend_key_data),
                ('I', self.handle_empty_query_response),
                ('n', self.handle_nodata),
            ]}
        self._status_parameters: Dict[str, str] = {}
        self._iso_dates = False
        self._tz_info: Optional[ZoneInfo] = None
        self._backend: Optional[Tuple[int, int]] = None
        self.password: Union[None, str, bytes] = None
        self.user: Union[None, str, bytes] = None

    @property
    def transaction_status(self) -> TransactionStatus:
        return TransactionStatus(self._transaction_status)

    @property
    def status(self) -> ProtocolStatus:
        return ProtocolStatus(self._status)

    @property
    def tz_info(self) -> Union[None, ZoneInfo]:
        return self._tz_info

    def handle_message(self, identifier: int, buf: memoryview) -> None:
        self._handlers[identifier](buf)

    def _startup_message(
            self, user: Union[str, bytes], database: Optional[str],
            application_name: Optional[str], tz_name: Optional[str],
            password: Union[None, str, bytes]) -> List[bytes]:
        parameters = []
        struct_format = ["!ii"]

        for name, value in [
                ("user", user),
                ("database", database),
                ("application_name", application_name),
                ("timezone", tz_name),
                ("DateStyle", "ISO"),
                ("client_encoding", "UTF8\0")]:

            if not value:
                continue
            bname = name.encode()
            if isinstance(value, str):
                bvalue = value.encode()
            else:
                bvalue = value
            struct_format.append(f"{len(bname) + 1}s{len(bvalue) + 1}s")
            parameters.extend([bname, bvalue])

        msg_struct = Struct(''.join(struct_format))
        message = msg_struct.pack(msg_struct.size, 196608, *parameters)

        self.user = user
        self.password = password
        self._status = _STATUS_STARTING_UP
        return [message]

    def terminate_message(self) -> bytes:
        self._status = _STATUS_CLOSING
        return b'X\x00\x00\x00\x04'

    def handle_error(self, buf: memoryview) -> None:
        # format: "({error_field_code:char}{error_field_value}\0)+\0"
        if buf[-2:] != b'\0\0':
            raise ProtocolError("Invalid Error Response")
        all_messages = decode(buf[:-2])
        messages = {msg[:1]: msg[1:] for msg in all_messages.split('\0')}
        ex_args: List[Any] = [None] * 17

        try:
            messages.pop('S')
        except KeyError:
            raise ProtocolError(
                "Missing localized severity 'S' in Error Response")
        try:
            severity_str = messages.pop('V')
        except KeyError:
            raise ProtocolError(
                "Missing severity 'V' in Error Response")
        try:
            severity = Severity(severity_str)
        except ValueError:
            raise ProtocolError(
                f"Unknown severity '{severity_str}' in Error Response")
        ex_args[0] = severity

        v: Union[int, str]
        for k, v in messages.items():
            if k in ('p', 'P', 'L'):
                try:
                    v = int(v)
                except Exception:
                    pass
            try:
                idx = _error_fields[k]
            except KeyError:
                continue
            ex_args[idx] = v

        if ex_args[1] is None:
            raise ProtocolError("Missing code in Error Response")
        if ex_args[2] is None:
            raise ProtocolError("Missing message in Error Response")

        if ex_args[16] == "RevalidateCachedQuery":
            # recognize this particular error, to easily handle retry
            ex_class: Type[ServerError] = CachedQueryExpired
        else:
            ex_class = ServerError
        ex = ex_class(*ex_args)

        if severity == Severity.FATAL or severity == Severity.PANIC:
            self._close()
            self._set_exception(ex)
        elif self._ex is None:
            # non fatal and connected, raise when ready for query arrives
            self._ex = ex

    def handle_auth_req(self, msg_buf: memoryview) -> None:
        # clear password from the object
        password = self.password
        self.password = None

        specifier, = int_struct_unpack_from(msg_buf)
        if specifier == 0:
            check_length_equal(4, msg_buf)
        elif specifier == 5:
            check_length_equal(8, msg_buf)
            user = self.user
            if password is None:
                raise ProtocolError("Missing password")
            if user is None:
                raise ProtocolError("Missing user")
            salt, = unpack_from("4s", msg_buf, 4)
            if isinstance(password, str):
                password = password.encode()
            if isinstance(user, str):
                user = user.encode()
            password = (
                b'md5' + md5(
                    md5(password + user).hexdigest().encode() + salt
                ).hexdigest().encode())

            pw_len = len(password) + 1
            struct_fmt = f'!ci{pw_len}s'
            self._set_result(
                [pack(struct_fmt, b'p', pw_len + 4, password)], False)
        else:
            raise ProtocolError(
                f"Unknown authentication specifier: {specifier}")

    def handle_parameter_status(self, msg_buf: memoryview) -> None:
        # format: "{param_name}\0{param_value}\0"

        param = bytes(msg_buf)
        param_parts = param.split(b'\0')
        if len(param_parts) != 3 or param_parts[2] != b'':
            raise ProtocolError("Invalid parameter status message")
        b_name, b_val = param_parts[:2]
        if b_name == b"client_encoding" and b_val != b'UTF8':
            raise InvalidOperationError(
                "The pagio library only works with 'UTF-8' encoding")
        name = decode(b_name)
        val = decode(b_val)
        if name == "DateStyle":
            self._iso_dates = val.startswith("ISO,")
        elif name == "TimeZone":
            try:
                self._tz_info = ZoneInfo(val)
            except ZoneInfoNotFoundError:
                self._tz_info = None
        self._status_parameters[name] = val

    def handle_backend_key_data(self, msg_buf: memoryview) -> None:
        self._backend = cast(Tuple[int, int], intint_struct.unpack(msg_buf))

    def handle_nodata(self, msg_buf: memoryview) -> None:
        check_length_equal(0, msg_buf)

    def handle_empty_query_response(self, msg_buf: memoryview) -> None:
        check_length_equal(0, msg_buf)


ParamConverter = Callable[[Any, int], Tuple[int, str, Any, int, Format]]
ResConverter = Callable[[memoryview], Any]
CacheKey = Union[str, Tuple[str, Tuple[int]]]


class PyBasePGProtocol(_AbstractPGProtocol):
    """ Pure Python functionality for both sync and async protocol """

    _handlers: Dict[int, Callable[[memoryview], None]]

    def __init__(self, *args: Tuple[Any]) -> None:

        # cache stuff
        self._cache: TypingOrderedDict[CacheKey, Dict[str, Any]] = OrderedDict()
        self._cache_item: Optional[Dict[str, Any]] = None
        self._prepare_threshold = 5
        self._cache_size = 100
        self.cache_key: Optional[CacheKey] = None

        # reading buffers and counters
        self._bytes_read = 0
        self._buf = self._standard_buf = memoryview(
            bytearray(STANDARD_BUF_SIZE))
        self._msg_len = 5
        self._identifier = None

        # resultset vars
        self.res_rows: Optional[List[Tuple[Any, ...]]] = None
        self.res_fields: Optional[List[FieldInfo]] = None
        self.res_converters: Optional[List[ResConverter]] = None

        # return values
        self._result: Optional[List[Tuple[Optional[List[FieldInfo]], Optional[List[Tuple[Any, ...]]], str]]] = None
        self._ex: Optional[ServerError] = None

        # status vars
        self._status = _STATUS_CLOSED
        self._transaction_status = 0

        super().__init__(*args)
        self._handlers.update({
            ord(k): v for k, v in [
                ('1', self.handle_parse_complete),
                ('2', self.handle_bind_complete),
                ('3', self.handle_close_complete),
                ('T', self.handle_row_description),
                ('D', self.handle_data_row),
                ('C', self.handle_command_complete),
                ('Z', self.handle_ready_for_query),
            ]})
        self.value_converters: Dict[int, Tuple[DBConverter, DBConverter]] = {
            INT2OID: (int, bin_int2_to_python),
            INT4OID: (int, bin_int_to_python),
            INT8OID: (int, bin_int8_to_python),
            FLOAT4OID: (float, bin_float4_to_python),
            FLOAT8OID: (float, bin_float8_to_python),
            BOOLOID: (text_bool_to_python, bin_bool_to_python),
            NAMEOID: (decode, decode),
            OIDOID: (int, bin_uint_to_python),
            CHAROID: (decode, decode),
            TEXTOID: (decode, decode),
            VARCHAROID: (decode, decode),
            BPCHAROID: (decode, decode),
        }
        self.param_converters: Dict[Type[Any], ParamConverter] = {
            int: int_to_pg,
            str: str_to_pg,
            type(None): none_to_pg,
            float: float_to_pg,
            bool: bool_to_pg,
        }

    def get_buffer(self, sizehint: int) -> memoryview:
        buf = self._buf
        if self._bytes_read:
            buf = buf[self._bytes_read:]
        return buf

    def buffer_updated(self, nbytes: int) -> None:
        self._bytes_read += nbytes
        msg_start = 0

        while self._bytes_read >= self._msg_len:
            if self._identifier is None:
                self._identifier, new_msg_len = header_struct_unpack_from(
                    self._standard_buf, msg_start)
                new_msg_len -= 4
                if new_msg_len < 0:
                    raise ProtocolError("Negative message length")
                if new_msg_len > STANDARD_BUF_SIZE:
                    self._buf = memoryview(bytearray(new_msg_len))
            else:
                self.handle_message(
                    self._identifier,
                    self._buf[msg_start:msg_start + self._msg_len])
                self._buf = self._standard_buf
                new_msg_len = 5
                self._identifier = None

            self._bytes_read -= self._msg_len
            msg_start += self._msg_len
            self._msg_len = new_msg_len

        if self._bytes_read and msg_start:
            # move incomplete trailing message part to start of buffer
            self._buf[:self._bytes_read] = (
                self._standard_buf[msg_start:msg_start + self._bytes_read])

    def convert_param(self, param: Any) -> Tuple[int, str, Any, int, Format]:
        return self.param_converters.get(type(param), default_to_pg)(param, 0)

    def _close_statement_msg(self, stmt_name: bytes) -> bytes:
        name_len = len(stmt_name)
        return pack(
            f"!cic{name_len + 1}s", b"C", 6 + name_len, b'S', stmt_name)

    def _simple_query_msg(self, sql: str) -> bytes:
        sql_bytes = sql.encode()
        sql_len = len(sql_bytes)
        return pack(f"!ci{sql_len + 1}s", b'Q', sql_len + 5, sql_bytes)

    def _parse_msg(
            self, sql: str, stmt_name: bytes, param_oids: Tuple[int, ...]
        ) -> bytes:
        sql_bytes = sql.encode()
        sql_len = len(sql_bytes)
        stmt_name_len = len(stmt_name)
        num_params = len(param_oids)

        return pack(
            f"!ci{stmt_name_len + 1}s{sql_len + 1}sH{num_params}I",
            b"P", stmt_name_len + sql_len + 8 + num_params * 4,
            stmt_name, sql_bytes, num_params, *param_oids)

    def _bind_msg(
            self,
            stmt_name: bytes,
            param_vals: Tuple[bytes],
            param_structs: Tuple[str],
            param_lens: Tuple[int],
            param_fmts: Tuple[int],
            result_format: Format,
        ) -> bytes:

        if result_format == Format.DEFAULT:
            result_format = Format.BINARY
        stmt_name_len = len(stmt_name)
        num_params = len(param_fmts)
        bind_length = stmt_name_len + 14 + num_params * 6

        param_pg_vals: List[Any] = []
        param_pg_fmts = []
        if num_params:
            for param_val, param_struct, param_len in zip(
                    param_vals, param_structs, param_lens):
                # Add param values, first length, then actual value
                param_pg_fmts.append("i")
                param_pg_vals.append(param_len)
                if param_len == -1:
                    # NULL
                    continue
                param_pg_fmts.append(param_struct)
                param_pg_vals.append(param_val)
                bind_length += param_len

        return pack(
            f"!cis{stmt_name_len + 1}s{num_params + 2}H"
            f"{''.join(param_pg_fmts)}HH",
            b"B", bind_length, b'', stmt_name, num_params,
            *param_fmts, num_params, *param_pg_vals, 1, result_format)

    def execute_message(
            self,
            sql: str,
            parameters: Tuple[Any, ...],
            result_format: Format,
    ) -> List[bytes]:
        message = []

        param_oids: Tuple[int]
        if parameters:
            param_oids, param_structs, param_vals, param_lens, param_fmts = zip(
                *(self.convert_param(p) for p in parameters))
        else:
            param_oids = cast(Tuple[int], ())
            param_structs = param_vals = param_lens = param_fmts = ()

        stmt_name = b''

        prepared = False
        cache_item = None
        if self._prepare_threshold:
            cache_key: CacheKey = (sql, param_oids) if parameters else sql
            cache_item = self._cache.get(cache_key)
            if cache_item:
                # statement executed before, move to end of cache
                # self._cache.move_to_end(cache_key)
                if cache_item["prepared"]:
                    # statement is prepared server side
                    # Reuse server side statement and skip parsing
                    prepared = True
                    stmt_name = cache_item["name"]
                else:
                    if cache_item["num_executed"] == self._prepare_threshold:
                        # Using a non-empty statement name for reuse
                        stmt_name = cache_item["name"]
            else:
                cache_item = {
                    "prepared": False,
                    "num_executed": 0,
                    "res_fields": None,
                    "res_converters": None,
                    "name": None,
                }

        if (not parameters
                and result_format in (Format.TEXT, Format.DEFAULT)
                and not prepared
                and not stmt_name):
            # Use simple query
            message.append(self._simple_query_msg(sql))
        else:
            if not prepared:
                # Parse
                message.append(self._parse_msg(sql, stmt_name, param_oids))

            # Bind
            message.append(self._bind_msg(
                stmt_name, param_vals, param_structs, param_lens, param_fmts,
                result_format))

            if not prepared:
                # Describe
                message.append(b'D\x00\x00\x00\x06P\x00')

            # Execute and Sync
            message.append(
                b'E\x00\x00\x00\t\x00\x00\x00\x00\x00S\x00\x00\x00\x04')

        # Set up for results
        if cache_item is not None:
            self.cache_key = cache_key
            self._cache_item = cache_item
            if prepared:
                # initialize result from cache
                self.res_fields = cache_item["res_fields"]
                self.res_converters = cache_item["res_converters"]
                self.res_rows = None if self.res_fields is None else []
        self._result = []
        self._status = _STATUS_EXECUTING

        return message

    def handle_parse_complete(self, msg_buf: memoryview) -> None:
        check_length_equal(0, msg_buf)
        if (self._cache_item is not None and
                self._cache_item["num_executed"] == self._prepare_threshold):
            self._cache_item["prepared"] = True

    def handle_bind_complete(self, msg_buf: memoryview) -> None:
        check_length_equal(0, msg_buf)

    def handle_close_complete(self, msg_buf: memoryview) -> None:
        check_length_equal(0, msg_buf)
        if self._cache_item is not None:
            # Reset the statement
            self._cache_item.update(num_executed=0, prepared=False)

    def handle_nodata(self, msg_buf: memoryview) -> None:
        check_length_equal(0, msg_buf)

    def handle_row_description(self, msg_buf: memoryview) -> None:
        buffer = bytes(msg_buf)
        res_fields = []
        converters = []
        num_fields, = ushort_struct_unpack_from(msg_buf)

        offset = 2
        for _ in range(num_fields):
            try:
                zero_idx = buffer.index(0, offset)
            except ValueError:
                raise ProtocolError("Invalid row description")
            field_name = decode(msg_buf[offset:zero_idx])
            offset = zero_idx + 1
            table_oid, col_num, type_oid, type_size, type_mod, _format = (
                field_desc_struct_unpack_from(msg_buf, offset))
            res_fields.append(FieldInfo(
                field_name, table_oid, col_num, type_oid, type_size, type_mod,
                _format))
            converters.append(
                self.value_converters.get(type_oid, _default_converters)[_format])
            offset += field_desc_struct_size
        if offset != len(msg_buf):
            raise ProtocolError("Additional data after row description")
        self.res_fields = res_fields
        self.res_rows = []
        self.res_converters = converters
        if self._cache_item is not None and self._cache_item["prepared"]:
            self._cache_item.update(
                res_fields=res_fields, res_converters=converters)

    def handle_data_row(self, buf: memoryview) -> None:
        if self.res_converters is None:
            raise ProtocolError("Unexpected data row.")
        value_converters = self.res_converters
        if ushort_struct_unpack_from(buf)[0] != len(value_converters):
            raise ProtocolError("Invalid number of row values")

        def get_vals() -> Generator[Any, None, None]:
            offset = 2
            for converter in value_converters:
                val_len = int_struct_unpack_from(buf, offset)[0]
                offset += 4
                if val_len == -1:
                    yield None
                elif val_len < 0:
                    raise ProtocolError("Negative length value")
                else:
                    val = converter(buf[offset:offset + val_len])
                    offset += val_len
                    yield val
            if offset != len(buf):
                raise ProtocolError("Additional data after data row")

        if self.res_rows is None:
            raise ProtocolError("Unexpected data row")
        self.res_rows.append(tuple(get_vals()))

    def handle_command_complete(self, msg_buf: memoryview) -> None:
        if msg_buf[-1] != 0:
            raise ProtocolError("Invalid command complete message")
        if self._result is None:
            raise ProtocolError("Unexpected close message.")
        self._result.append((
            self.res_fields, self.res_rows, decode(msg_buf[:-1])))
        self.res_fields = None
        self.res_converters = None
        self.res_rows = None

    def handle_ready_for_query(self, msg_buf: memoryview) -> None:
        self._transaction_status = single_byte_struct_unpack(msg_buf)[0]
        self._status = _STATUS_READY_FOR_QUERY

        cache_item = self._cache_item
        if cache_item:
            if self.cache_key is None:
                raise ProtocolError("Unexpected ready for query.")
            if self._ex is not None:
                if cache_item["prepared"]:
                    cache_item.update(res_fields=None, res_converters=None)
                    self._set_result([
                        self._close_statement_msg(cache_item["name"]),
                        b'S\x00\x00\x00\x04',
                        ], False)
                    return
            elif (not cache_item["prepared"] and self._result
                    and len(self._result) == 1):
                # we have a non-prepared statement that is eligible for server
                # side preparing
                if cache_item["num_executed"] == 0:
                    # new statement, add to cache
                    cache_len = len(self._cache)
                    if cache_len == self._cache_size:
                        # Cache is full. Remove old statement, reuse statement
                        # name and close old statement
                        old_item = self._cache.popitem(last=False)[1]
                        cache_item["name"] = old_item["name"]
                        if old_item["prepared"]:
                            # Close old statement
                            self._set_result([
                                self._close_statement_msg(old_item["name"]),
                                b'S\x00\x00\x00\x04'
                                ], False)
                            return
                    elif not cache_item["name"]:
                        # generate cache name
                        cache_item["name"] = f"_pagio_{cache_len:03}".encode()
                    # add statement to cache
                    self._cache[self.cache_key] = cache_item
                else:
                    self._cache.move_to_end(self.cache_key)
                cache_item["num_executed"] += 1
            self._cache_item = None
            self.cache_key = None

        if self._ex is not None:
            self._set_exception(self._ex)
            self._ex = None
        else:
            self._set_result(self._result, True)
        self._result = None
