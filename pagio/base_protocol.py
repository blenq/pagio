from abc import abstractmethod, ABC
from codecs import decode
import enum
from hashlib import md5
from struct import Struct, unpack_from, pack
from typing import (
    Optional, Union, Dict, Callable, List, Any, Tuple, cast, Generator,
    Sequence, Type)

from .common import (
    ProtocolError, Severity, _error_fields, ServerError, InvalidOperationError,
    ResultSet, FieldInfo,
)
from .const import *
from .zoneinfo import ZoneInfo, ZoneInfoNotFoundError

STANDARD_BUF_SIZE = 0x4000


header_struct_unpack_from = Struct("!Bi").unpack_from
ushort_struct_unpack_from = Struct('!H').unpack_from
int2_struct = Struct('!h')
int2_struct_unpack = int2_struct.unpack
int_struct = Struct('!i')
# int_struct_pack = int_struct.pack
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
    TEXT = 0
    BINARY = 1


_default_converters = [decode, bytes]


def none_to_pg(val: None, oid: int) -> Tuple[int, str, None, int, Format]:
    return oid, "", None, 0, Format.TEXT


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


class _BasePGProtocol:
    res_converters: List[DBConverter]

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
        self._integer_datetimes = False
        self._tz_info: Optional[ZoneInfo] = None
        self._backend: Optional[Tuple[int, int]] = None
        # self.res_fields: Optional[List[Dict[str, Any]]] = None
        # self.res_rows: Optional[List[Tuple[Any, ...]]] = None
        self.password: Union[None, str, bytes] = None
        self.user: Union[None, str, bytes] = None

    @abstractmethod
    def _set_result(self) -> None:
        ...

    @abstractmethod
    def _set_exception(self, ex: BaseException) -> None:
        ...

    @abstractmethod
    def _close(self) -> None:
        ...

    @property
    def transaction_status(self) -> TransactionStatus:
        return TransactionStatus(self._transaction_status)

    @property
    def status(self) -> ProtocolStatus:
        return ProtocolStatus(self._status)

    @property
    def tz_info(self) -> Union[None, ZoneInfo]:
        return self._tz_info

    def handle_message(self, identifier, buf):
        self._handlers[identifier](buf)

    def _startup_message(
            self, user: Union[str, bytes], database: Optional[str],
            application_name: Optional[str], tz_name: Optional[str],
            password: Union[None, str, bytes]) -> bytes:
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
        return message

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
        ex = ServerError(*ex_args)

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
            self._result = pack(struct_fmt, b'p', pw_len + 4, password)
            self._set_result()
            self._result = None
        else:
            raise ProtocolError(
                f"Unknown authentication specifier: {specifier}")

    def handle_parameter_status(self, msg_buf: memoryview) -> None:
        # format: "{param_name}\0{param_value}\0"

        param = decode(msg_buf)
        param_parts = param.split('\0')
        if len(param_parts) != 3 or param_parts[2] != '':
            raise ProtocolError("Invalid parameter status message")
        name, val = param_parts[:2]
        if name == "client_encoding":
            if val != 'UTF8':
                raise InvalidOperationError(
                    "The pagio library only works with 'UTF-8' encoding")
        elif name == "DateStyle":
            self._iso_dates = val.startswith("ISO,")
        elif name == "integer_datetimes":
            self._integer_datetimes = (val == 'on')
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


class PyBasePGProtocol(_BasePGProtocol, ABC):

    def __init__(self) -> None:
        self._bytes_read = 0
        self._buf = self._standard_buf = memoryview(
            bytearray(STANDARD_BUF_SIZE))
        self._msg_len = 5
        self._identifier = None
        self.res_rows = None
        self.res_fields = None
        self._status = _STATUS_CLOSED
        self._result: Any = None
        self._ex: Optional[ServerError] = None
        self._transaction_status = 0
        super().__init__()
        self._handlers.update({
            ord(k): v for k, v in [
                ('1', self.handle_parse_complete),
                ('2', self.handle_bind_complete),
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
        self.param_converters = {
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
                # print(chr(self._identifier))
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

    def execute_message(
            self,
            sql: str,
            parameters: Tuple[Any],
            result_format: Format,
    ) -> bytes:
        # self.res_fields = None
        # self.res_rows =
        self._status = _STATUS_EXECUTING
        self._result = ResultSet()
        sql_bytes = sql.encode()
        sql_len = len(sql_bytes)
        if not parameters and result_format is Format.TEXT:
            # Use simple query
            return pack(f"!ci{sql_len + 1}s", b'Q', sql_len + 5, sql_bytes)

        if parameters:
            param_oids, param_structs, param_vals, param_lens, param_fmts = zip(
                *(self.convert_param(p) for p in parameters))
        else:
            param_oids = param_structs = param_vals = param_fmts = param_lens = tuple()
        fmt = ["!"]
        cargs = []
        stmt_name = b''
        stmt_name_len = len(stmt_name)
        num_params = len(param_oids)

        # Parse
        fmt.append(
            f"ci{stmt_name_len + 1}s{sql_len + 1}sH{num_params}I")
        cargs.extend([
            b"P", stmt_name_len + sql_len + 8 + num_params * 4,
            stmt_name, sql_bytes, num_params])
        cargs.extend(param_oids)

        # Bind
        fmt.append(
            f"cis{stmt_name_len + 1}s{num_params + 2}H"
        )
        bind_length = stmt_name_len + 14 + num_params * 6

        param_pg_vals = []
        for param_val, param_struct, param_len in zip(
                param_vals, param_structs, param_lens):
            # Add param values, first length, then actual value
            fmt.append("i")
            if param_val is None:
                # Special case for None (pg NULL)
                param_pg_vals.append(-1)
            else:
                fmt.append(param_struct)
                param_pg_vals.extend([param_len, param_val])
                bind_length += param_len
        cargs.extend([
            b"B", bind_length, b'', stmt_name, num_params])
        cargs.extend(param_fmts)
        cargs.append(num_params)
        cargs.extend(param_pg_vals)

        # Bind trailer, Describe, Execute and Sync
        fmt.append(
            "HH"
            "cics"
            "cisi"
            "ci")
        cargs.extend([
            1, result_format,
            b"D", 6, b"P", b'',
            b'E', 9, b'', 0,
            b"S", 4])

        return pack("".join(fmt), *cargs)

    def handle_parse_complete(self, msg_buf: memoryview) -> None:
        check_length_equal(0, msg_buf)

    def handle_bind_complete(self, msg_buf: memoryview) -> None:
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

    def handle_data_row(self, buf: memoryview) -> None:
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
        self._result._add_result(
            self.res_fields, self.res_rows, decode(msg_buf[:-1]))
        self.res_fields = None
        self.res_rows = None

    def handle_ready_for_query(self, msg_buf: memoryview) -> None:
        self._transaction_status = single_byte_struct_unpack(msg_buf)[0]
        self._status = _STATUS_READY_FOR_QUERY

        if self._ex is not None:
            self._set_exception(self._ex)
            self.res_fields = None
            self.res_rows = None
            self._ex = None
        else:
            self._set_result()
        self._result = None


try:
    from ._pagio import CBasePGProtocol

    class BasePGProtocol(CBasePGProtocol, _BasePGProtocol):
        pass

except ImportError:
    BasePGProtocol = PyBasePGProtocol
