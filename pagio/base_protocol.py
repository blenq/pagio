""" Base protocol functionality """

from abc import abstractmethod, ABC
from codecs import decode
from collections import OrderedDict
from datetime import date, datetime
import enum
from hashlib import md5
from io import IOBase
from itertools import repeat
from struct import Struct, unpack_from, pack
from typing import (
    Optional, Union, Dict, Callable, List, Any, Tuple, cast, Generator,
    Type, OrderedDict as TypingOrderedDict, Iterable)

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict


from .pgscramp import PGScrampClient

from .common import (
    ProtocolError, Severity, _error_fields, ServerError, InvalidOperationError,
    FieldInfo, CachedQueryExpired, check_length_equal,
    ushort_struct_unpack_from, Format, StatementDoesNotExist,
)
from . import const
from .dt import (
    txt_date_to_python, bin_date_to_python, txt_timestamp_to_python,
    bin_timestamp_to_python, txt_timestamptz_to_python,
    bin_timestamptz_to_python)
from .network import (
    txt_inet_to_python, bin_inet_to_python, txt_cidr_to_python,
    bin_cidr_to_python)
from . import numeric
from .text import txt_bytea_to_python, txt_uuid_to_python, bin_uuid_to_python
from .zoneinfo import ZoneInfo, ZoneInfoNotFoundError

STANDARD_BUF_SIZE = 0x4000


msg_header_struct_unpack_from = Struct("!Bi").unpack_from

int_struct_unpack_from = Struct('!i').unpack_from

field_desc_struct = Struct("!IhIhih")
field_desc_struct_size = field_desc_struct.size
field_desc_struct_unpack_from = field_desc_struct.unpack_from

intint_struct = Struct('!ii')

single_byte_struct_unpack = Struct('!B').unpack


_STATUS_CLOSED = 0
_STATUS_CLOSING = 1
_STATUS_CONNECTED = 2
_STATUS_SSL_REQUESTED = 3
_STATUS_STARTING_UP = 4
_STATUS_READY_FOR_QUERY = 5
_STATUS_EXECUTING = 6


class ProtocolStatus(enum.IntEnum):
    """ Protocol status """
    CLOSED = _STATUS_CLOSED
    CLOSING = _STATUS_CLOSING
    CONNECTED = _STATUS_CONNECTED
    SSL_REQUESTED = _STATUS_SSL_REQUESTED
    STARTING_UP = _STATUS_STARTING_UP
    READY_FOR_QUERY = _STATUS_READY_FOR_QUERY
    EXECUTING = _STATUS_EXECUTING


class TransactionStatus(enum.Enum):
    """ Transaction status """
    UNKNOWN = 0
    IDLE = ord('I')
    TRANSACTION = ord('T')
    ERROR = ord('E')


_default_converters = [decode, bytes]


# pylint: disable-next=unused-argument
def none_to_pg(val: None) -> Tuple[int, str, None, int, Format]:
    """ Parameter values for None """
    return 0, "", None, -1, Format.TEXT


def str_to_pg(val: str) -> Tuple[int, str, bytes, int, Format]:
    """ Convert a Python string to a PG text parameter """
    bytes_val = val.encode()
    val_len = len(bytes_val)
    return 0, f"{val_len}s", bytes_val, val_len, Format.TEXT


def default_to_pg(val: Any) -> Tuple[int, str, bytes, int, Format]:
    """ Convert a Python object to a PG text parameter """
    return str_to_pg(str(val))


def int_to_pg(val: int) -> Tuple[int, str, Union[int, bytes], int, Format]:
    """ Convert a Python int to a PG int parameter """
    if -0x10000000 <= val <= 0x7FFFFFFF:
        return const.INT4OID, "i", val, 4, Format.BINARY
    if -0x1000000000000000 <= val <= 0x7FFFFFFFFFFFFFFF:
        return const.INT8OID, "q", val, 8, Format.BINARY
    return default_to_pg(val)


def bool_to_pg(val: bool) -> Tuple[int, str, bool, int, Format]:
    """ Convert a Python bool to a PG bool parameter """
    return const.BOOLOID, "B", val, 1, Format.BINARY


DBConverter = Callable[[memoryview], Any]


class _AbstractPGProtocol(ABC):
    _prepare_threshold: int
    _cache_size: int

    @abstractmethod
    def _set_result(self, result: Any) -> None:
        ...

    @abstractmethod
    def _set_exception(self, ex: BaseException) -> None:
        ...

    @abstractmethod
    def _close(self) -> None:
        ...

    @abstractmethod
    def get_buffer(self, sizehint: int) -> memoryview:
        """ Gets a buffer to receive data into. """

    @abstractmethod
    def buffer_updated(self, nbytes: int) -> None:
        """ Notify buffer is updated with received data. """

    @abstractmethod
    def execute_message(
        self,
        sql: str,
        parameters: Tuple[Any, ...],
        result_format: Format,
        raw_result: bool,
        file_obj: IOBase,
    ) -> List[bytes]:
        """ Executes a statement. """

    @abstractmethod
    def handle_message(self, identifier: int, buf: memoryview) -> None:
        """ Handle a received message """

    @abstractmethod
    def _setup_ssl_request(self) -> None:
        ...

    @abstractmethod
    def get_channel_binding(self) -> Optional[Tuple[str, bytes]]:
        ...


CANCEL_REQUEST_CODE = 80877102


# pylint: disable-next=too-many-instance-attributes
class _BasePGProtocol(_AbstractPGProtocol):
    """ Common functionality for pure python and c accelerated versions of
    the PG protocol class, sync and async.

    """
    res_converters: Optional[List[DBConverter]]
    _transaction_status: int
    _ex: Optional[ServerError]
    _status: int
    _server_parameters: Dict[str, str]
    _tz_info: Optional[ZoneInfo]

    def __init__(self) -> None:
        self._handlers: Dict[int, Callable[[memoryview], None]] = {
            ord(k): v for k, v in [
                (' ', self.handle_ssl_response),
                ('E', self.handle_error),
                ('R', self.handle_auth_req),
                ('K', self.handle_backend_key_data),
                ('I', self.handle_empty_query_response),
                ('n', self.handle_nodata),
            ]}
        self._backend: Optional[Tuple[int, int]] = None
        self.password: Union[None, bytes] = None
        self.user: Union[None, bytes] = None
        self.scram_client: Optional[PGScrampClient] = None
        self._ssl_in_use = False

    @property
    def ssl_in_use(self) -> bool:
        """ Indicates if SSL is used. """
        return self._ssl_in_use

    @property
    def transaction_status(self) -> TransactionStatus:
        """ Transaction status """
        return TransactionStatus(self._transaction_status)

    @property
    def server_parameters(self) -> Dict[str, str]:
        """ Server parameters """
        return self._server_parameters

    @property
    def status(self) -> ProtocolStatus:
        """ Protocol status """
        return ProtocolStatus(self._status)

    @property
    def tz_info(self) -> Union[None, ZoneInfo]:
        """ Session timezone """
        return self._tz_info

    @property
    def backend_key(self) -> Tuple[int, int]:
        """ The backend key data, to send a Cancel Request """
        if self._backend is None:
            raise ValueError("No backend key")
        return self._backend

    def handle_message(self, identifier: int, buf: memoryview) -> None:
        """ Handle a received message """
        self._handlers[identifier](buf)

    def _startup_message(  # pylint: disable=too-many-arguments
            self, user: Union[str, bytes], database: Optional[str],
            application_name: Optional[str], tz_name: Optional[str],
            password: Union[None, str, bytes], prepare_threshold: int,
            cache_size: int
    ) -> bytes:
        parameters = []
        struct_format = ["!ii"]

        if isinstance(user, str):
            user = user.encode()

        for bname, value in [
                (b"user", user),
                (b"database", database),
                (b"application_name", application_name),
                (b"timezone", tz_name),
                (b"DateStyle", "ISO"),
                (b"client_encoding", "UTF8\0")]:

            if not value:
                continue
            if isinstance(value, str):
                value = value.encode()
            struct_format.append(f"{len(bname) + 1}s{len(value) + 1}s")
            parameters.extend([bname, value])

        msg_struct = Struct(''.join(struct_format))
        message = msg_struct.pack(msg_struct.size, 0x30000, *parameters)

        self.user = user
        if isinstance(password, str):
            password = password.encode()
        self.password = password
        self._prepare_threshold = prepare_threshold
        self._cache_size = cache_size
        self._status = _STATUS_STARTING_UP
        return message

    def cancel_message(self, backend_key: Tuple[int, int]) -> bytes:
        """ Returns a Cancel Request message. """
        return pack("!iiii", 16, CANCEL_REQUEST_CODE, *backend_key)

    def terminate_message(self) -> bytes:
        """ Gets a terminate client message. """
        self._status = _STATUS_CLOSING
        return b'X\x00\x00\x00\x04'

    def handle_ssl_response(self, msg_buf: memoryview) -> None:
        """ Handles response to SSL request """
        check_length_equal(1, msg_buf)
        res = msg_buf[0]
        if res == 83:  # 'S'
            self._set_result(True)
        elif res == 78:  # 'N'
            self._set_result(False)
        else:
            raise ProtocolError("Unexpected response from server")

    def _get_ex_val(self, messages: Dict[str, str], key: str) -> str:
        try:
            return messages.pop(key)
        except KeyError:
            # pylint: disable-next=raise-missing-from
            raise ProtocolError(f"Missing key '{key}' in Error Response.")

    def handle_error(self, buf: memoryview) -> None:
        """ Interprets and sets a server error. """
        # format: "({error_field_code:char}{error_field_value}\0)+\0"
        if buf[-2:] != b'\0\0':
            raise ProtocolError("Invalid Error Response")
        messages = {msg[:1]: msg[1:] for msg in decode(buf[:-2]).split('\0')}
        ex_args: List[Union[Severity, str, int, None]] = [None] * 17

        self._get_ex_val(messages, 'S')
        ex_args[0] = Severity(self._get_ex_val(messages, 'V'))

        value: Union[int, str]
        for k, value in messages.items():
            if k in ('p', 'P', 'L'):
                try:
                    value = int(value)
                except ValueError:
                    pass
            try:
                idx = _error_fields[k]
            except KeyError:
                continue
            ex_args[idx] = value

        if ex_args[1] is None:
            raise ProtocolError("Missing code in Error Response")
        if ex_args[2] is None:
            raise ProtocolError("Missing message in Error Response")

        if ex_args[16] == "RevalidateCachedQuery":
            # recognize this particular error, to easily handle retry
            ex_class: Type[ServerError] = CachedQueryExpired
        elif ex_args[1] == "26000":
            # recognize this particular error, to easily handle retry
            ex_class = StatementDoesNotExist
        else:
            ex_class = ServerError
        exc = ex_class(*ex_args)

        if ex_args[0] is Severity.FATAL or ex_args[0] is Severity.PANIC:
            self._close()
            self._set_exception(exc)
        elif self._ex is None:
            # non fatal and connected, raise when ready for query arrives
            self._ex = exc

    def _handle_md5_auth_req(self, msg_buf: memoryview) -> None:
        check_length_equal(8, msg_buf)
        if self.password is None:
            raise ProtocolError("Missing password")
        if self.user is None:
            raise ProtocolError("Missing user")
        salt, = unpack_from("4s", msg_buf, 4)
        pw_hash = (b'md5' + md5(md5(
            self.password + self.user
        ).hexdigest().encode() + salt).hexdigest().encode())

        pw_len = len(pw_hash) + 1
        struct_fmt = f'!ci{pw_len}s'
        self._set_result(
            pack(struct_fmt, b'p', pw_len + 4, pw_hash))

    def handle_auth_req(self, msg_buf: memoryview) -> None:
        """ Handles authentication messages """
        specifier, = int_struct_unpack_from(msg_buf)
        if specifier == 0:
            check_length_equal(4, msg_buf)
        elif specifier == 5:
            self._handle_md5_auth_req(msg_buf)
        elif specifier == 10:
            if self.password is None:
                raise ProtocolError("Missing password")
            # SASL auth
            if len(msg_buf) < 6 or msg_buf[-2:] != b'\0\0':
                raise ProtocolError("Invalid SASL message.")
            mechanisms = decode(msg_buf[4:-2]).split("\0")
            self.scram_client = PGScrampClient(
                mechanisms, self.password, self.get_channel_binding())

            client_first = self.scram_client.get_client_first().encode()
            mechanism = self.scram_client.mechanism_name.encode()
            cf_len = len(client_first)
            mech_len = len(mechanism)
            self._set_result(pack(
                f"!ci{mech_len + 1}si{cf_len}s",
                b'p', mech_len + cf_len + 9, mechanism, cf_len, client_first
            ))
        elif specifier == 11:
            # SASL continue
            if self.scram_client is None:
                raise ProtocolError("Unexpected SASL continue message.")

            self.scram_client.set_server_first(decode(msg_buf[4:]))
            msg = self.scram_client.get_client_final()
            msg_bytes = msg.encode()
            msg_len = len(msg_bytes)
            self._set_result(pack(
                f"!ci{msg_len}s", b'p', msg_len + 4, msg_bytes))
        elif specifier == 12:
            # SASL final
            if self.scram_client is None:
                raise ProtocolError("Unexpected SASL final message.")

            self.scram_client.set_server_final(decode(msg_buf[4:]))

            # reset scram vars
            self.scram_client = None
        else:
            raise ProtocolError(
                f"Unknown authentication specifier: {specifier}")

    def handle_backend_key_data(self, msg_buf: memoryview) -> None:
        """ Handles the backend key """
        self._backend = cast(Tuple[int, int], intint_struct.unpack(msg_buf))

    def handle_nodata(self, msg_buf: memoryview) -> None:
        """ Handles a nodata message """
        check_length_equal(0, msg_buf)

    def handle_empty_query_response(self, msg_buf: memoryview) -> None:
        """ Handles an empty query response. """
        check_length_equal(0, msg_buf)


ParamConverter = Callable[[Any], Tuple[int, str, Any, int, Format]]
ResConverter = Callable[[memoryview], Any]
CacheKey = Union[str, Tuple[str, Tuple[int]]]


class Statement(TypedDict):
    prepared: bool
    num_executed: int
    res_fields: Optional[Tuple[FieldInfo, ...]]
    bin_converters: Optional[List[ResConverter]]
    txt_converters: Optional[List[ResConverter]]
    name: bytes


# pylint: disable-next=too-many-instance-attributes
class PyBasePGProtocol(_AbstractPGProtocol):
    """ Pure Python functionality for both sync and async protocol """

    _handlers: Dict[int, Callable[[memoryview], None]]

    def __init__(self, *args: Tuple[Any]) -> None:

        # cache stuff
        self._cache: TypingOrderedDict[
            CacheKey, Statement] = OrderedDict()
        self._cache_item: Optional[Statement] = None
        self._prepare_threshold = 5
        self._cache_size = 100
        self.cache_key: Optional[CacheKey] = None
        self._stmt_to_close: Optional[Dict[str, Any]] = None

        # reading buffers and counters
        self._bytes_read = 0
        self._buf = self._standard_buf = memoryview(
            bytearray(STANDARD_BUF_SIZE))
        self._msg_part_len = 5
        self._identifier: Optional[int] = None

        # resultset vars
        self.res_rows: Optional[List[Tuple[Any, ...]]] = None
        self.res_fields: Optional[Tuple[FieldInfo, ...]] = None
        self.res_converters: Optional[List[ResConverter]] = None
        self._result_format = Format.DEFAULT
        self._raw_result = False
        self._extended_query = False

        # return values
        self._result: Optional[List[Tuple[
            Optional[List[FieldInfo]],
            Optional[List[Tuple[Any, ...]]],
            str,
        ]]] = None
        self._ex: Optional[ServerError] = None

        # status vars
        self._status = _STATUS_CLOSED
        self._transaction_status = 0
        self._server_parameters: Dict[str, str] = {}
        self._iso_dates = False
        self._tz_info: Optional[ZoneInfo] = None

        super().__init__(*args)
        self._handlers.update({
            ord(k): v for k, v in [
                ('S', self.handle_parameter_status),
                ('1', self.handle_parse_complete),
                ('2', self.handle_bind_complete),
                ('3', self.handle_close_complete),
                ('T', self.handle_row_description),
                ('D', self.handle_data_row),
                ('C', self.handle_command_complete),
                ('Z', self.handle_ready_for_query),
            ]})
        self.value_converters: Dict[int, Tuple[DBConverter, DBConverter]] = {
            const.INT2OID: (int, numeric.bin_int2_to_python),
            const.INT4OID: (int, numeric.bin_int_to_python),
            const.INT8OID: (int, numeric.bin_int8_to_python),
            const.FLOAT4OID: (float, numeric.bin_float4_to_python),
            const.FLOAT8OID: (float, numeric.bin_float8_to_python),
            const.BOOLOID: (
                numeric.text_bool_to_python, numeric.bin_bool_to_python),
            const.NUMERICOID: (
                numeric.txt_numeric_to_python, numeric.bin_numeric_to_python),
            const.NAMEOID: (decode, decode),
            const.OIDOID: (int, numeric.bin_uint_to_python),
            const.CHAROID: (decode, decode),
            const.TEXTOID: (decode, decode),
            const.VARCHAROID: (decode, decode),
            const.BPCHAROID: (decode, decode),
            const.BYTEAOID: (txt_bytea_to_python, bytes),
            const.INETOID: (txt_inet_to_python, bin_inet_to_python),
            const.CIDROID: (txt_cidr_to_python, bin_cidr_to_python),
            const.UUIDOID: (txt_uuid_to_python, bin_uuid_to_python),
            const.DATEOID: (self.txt_date_to_python, bin_date_to_python),
            const.TIMESTAMPOID: (
                self.txt_timestamp_to_python, bin_timestamp_to_python),
            const.TIMESTAMPTZOID: (
                self.txt_timestamptz_to_python,
                self.bin_timestamptz_to_python,
            ),
        }
        self.param_converters: Dict[Type[Any], ParamConverter] = {
            int: int_to_pg,
            str: str_to_pg,
            type(None): none_to_pg,
            float: numeric.float_to_pg,
            bool: bool_to_pg,
        }

    def get_buffer(self, sizehint: int) -> memoryview:
        """ Gets a buffer to receive data into. """
        buf = self._buf
        if self._bytes_read:
            buf = buf[self._bytes_read:]
        return buf

    def buffer_updated(self, nbytes: int) -> None:
        """ Notify buffer is updated with received data. """
        # PostgreSQL message contains of a fixed 5 byte header and optional
        # content:
        #   header: 1 byte identifier + 4 byte length of message
        self._bytes_read += nbytes
        msg_start = 0

        while self._bytes_read >= self._msg_part_len:
            # read in two stages, first header, then content
            if self._identifier is None:
                # read header
                self._identifier, msg_len = msg_header_struct_unpack_from(
                    self._standard_buf, msg_start)

                # msg_len includes msg_len itself, so subtract 4
                msg_part_len = msg_len - 4
                if msg_part_len < 0:
                    raise ProtocolError("Negative message length")

                if msg_part_len > STANDARD_BUF_SIZE:
                    # message does not fit in standard buf, allocate
                    # XL buffer
                    self._buf = memoryview(bytearray(msg_part_len))
            else:
                # content is present, handle the message
                self.handle_message(
                    self._identifier,
                    self._buf[msg_start:msg_start + self._msg_part_len])

                # if XL buffer was used, it is discarded now
                self._buf = self._standard_buf

                # set up for reading header again
                msg_part_len = 5
                self._identifier = None

            # set up for reading the next stage
            self._bytes_read -= self._msg_part_len
            msg_start += self._msg_part_len
            self._msg_part_len = msg_part_len

        if self._bytes_read and msg_start:
            # move incomplete trailing message part to start of buffer
            self._buf[:self._bytes_read] = (
                self._standard_buf[msg_start:msg_start + self._bytes_read])

    def _setup_ssl_request(self) -> None:
        self._identifier = 32  # pseudo identifier, not set by server
        self._msg_part_len = 1

    def convert_param(self, param: Any) -> Tuple[int, str, Any, int, Format]:
        """ Convert a Python value into a PostgreSQL param tuple. """
        return self.param_converters.get(type(param), default_to_pg)(param)

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

    def _bind_msg(  # pylint: disable=too-many-arguments
            self,
            stmt_name: bytes,
            param_vals: Tuple[bytes],
            param_structs: Tuple[str],
            param_lens: Tuple[int],
            param_fmts: Tuple[int],
            result_format: Format,
    ) -> bytes:

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

    def _check_cache(
            self, sql: str, param_oids: Tuple[int], result_format: Format
    ) -> Tuple[bytes, bool, Format]:

        stmt_name = b''
        prepared = False
        if self._prepare_threshold:
            self.cache_key = (sql, param_oids) if param_oids else sql
            self._cache_item = cache_item = self._cache.get(self.cache_key)
            if cache_item is not None:
                # statement executed before
                if cache_item["prepared"]:
                    # statement is prepared server side
                    if (self._stmt_to_close is None or
                            self._stmt_to_close["name"] != cache_item["name"]):
                        # statement is prepared server side
                        # Reuse server side statement and skip parsing
                        prepared = True
                        stmt_name = cache_item["name"]
                        self.res_fields = cache_item["res_fields"]
                else:
                    if cache_item["num_executed"] == self._prepare_threshold:
                        # Using a non-empty statement name for reuse
                        stmt_name = cache_item["name"]
        if result_format == Format.DEFAULT:
            if not param_oids and not prepared and not stmt_name:
                result_format = Format.TEXT
            else:
                result_format = Format.BINARY
        if prepared and self.res_fields is not None:
            if result_format == Format.TEXT:
                conv_attr = "txt_converters"
            else:
                conv_attr = "bin_converters"
            if cache_item[conv_attr] is None:
                cache_item[conv_attr] = [
                    self.value_converters.get(
                        f_info.type_oid, _default_converters
                    )[result_format] for f_info in self.res_fields]

            self.res_converters = cache_item[conv_attr]
            self.res_rows = []
        return stmt_name, prepared, result_format

    def execute_message(
            self,
            sql: str,
            parameters: Tuple[Any, ...],
            result_format: Format,
            raw_result: bool,
            file_obj: IOBase,
    ) -> List[bytes]:
        """ Executes a statement. """

        message = []

        if self._stmt_to_close is not None:
            message.append(
                self._close_statement_msg(self._stmt_to_close["name"]))

        if parameters:
            (param_oids, param_structs, param_vals, param_lens,
             param_fmts) = zip(
                *(self.convert_param(p) for p in parameters))
        else:
            param_oids = cast(Tuple[int, ...], ())
            param_structs = param_vals = param_lens = param_fmts = ()

        stmt_name, prepared, result_format = self._check_cache(
            sql, param_oids, result_format)

        if (not parameters
                and result_format == Format.TEXT
                and not prepared
                and not stmt_name):
            # Use simple query
            message.append(self._simple_query_msg(sql))
            self._extended_query = False
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
            self._extended_query = True

        self._result = []
        self._result_format = result_format
        self._raw_result = raw_result
        self.file_obj = file_obj

        return message

    def handle_parameter_status(self, msg_buf: memoryview) -> None:
        """ Handles a server parameter """
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
        self._server_parameters[name] = val

    def handle_parse_complete(self, msg_buf: memoryview) -> None:
        """ Handles a Parse Complete message """
        check_length_equal(0, msg_buf)
        if (self._cache_item is not None and
                self._cache_item["num_executed"] == self._prepare_threshold):
            self._cache_item["prepared"] = True

    def handle_bind_complete(self, msg_buf: memoryview) -> None:
        """ Handles a Bind Complete message """
        check_length_equal(0, msg_buf)

    def handle_close_complete(self, msg_buf: memoryview) -> None:
        """ Handles a Close Complete message """
        check_length_equal(0, msg_buf)
        if self._stmt_to_close is None:
            raise ProtocolError("Unexpected close complete message.")

        # Reset the statement
        self._stmt_to_close.update(
            prepared=False, num_executed=0, res_fields=None,
            txt_converters=None, bin_converters=None)
        self._stmt_to_close = None

    def handle_nodata(self, msg_buf: memoryview) -> None:
        """ Handles a nodata message """
        check_length_equal(0, msg_buf)

    def handle_row_description(self, msg_buf: memoryview) -> None:
        """ Handles a Row Description message. """
        buffer = bytes(msg_buf)
        res_fields = []
        converters = []
        num_fields, = ushort_struct_unpack_from(msg_buf)

        offset = 2
        for _ in range(num_fields):
            try:
                zero_idx = buffer.index(0, offset)
            except ValueError:
                # pylint: disable-next=raise-missing-from
                raise ProtocolError("Invalid row description")
            field_name = decode(msg_buf[offset:zero_idx])
            offset = zero_idx + 1
            table_oid, col_num, type_oid, type_size, type_mod, _format = (
                field_desc_struct_unpack_from(msg_buf, offset))
            res_fields.append(FieldInfo(
                field_name, table_oid, col_num, type_oid, type_size, type_mod,
                _format))
            converters.append(self.value_converters.get(
                type_oid, _default_converters)[_format])
            offset += field_desc_struct_size
        if offset != len(msg_buf):
            raise ProtocolError("Additional data after row description")
        self.res_fields = (*res_fields,)
        self.res_rows = []
        self.res_converters = converters
        if self._cache_item is not None and self._cache_item["prepared"]:
            # store field_info and converters in cache
            if _format == Format.TEXT:
                conv_attr = "txt_converters"
            else:
                conv_attr = "bin_converters"
            self._cache_item[conv_attr] = converters
            self._cache_item["res_fields"] = res_fields

    def handle_data_row(self, buf: memoryview) -> None:
        """ Handles a DataRow message. """
        if self.res_converters is None or self.res_rows is None:
            raise ProtocolError("Unexpected data row.")

        num_converters = len(self.res_converters)
        if ushort_struct_unpack_from(buf)[0] != num_converters:
            raise ProtocolError("Invalid number of row values")

        value_converters: Iterable[Callable[[memoryview], Any]]
        if self._raw_result:
            if self._result_format == Format.TEXT:
                value_converters = repeat(decode, num_converters)
            else:
                value_converters = repeat(bytes, num_converters)
        else:
            value_converters = self.res_converters

        def get_vals() -> Generator[Any, None, None]:
            offset = 2
            for converter in value_converters:
                val_len, = int_struct_unpack_from(buf, offset)
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

        self.res_rows.append(tuple(get_vals()))

    def handle_command_complete(self, msg_buf: memoryview) -> None:
        """ Handles a Command Complete message. """
        if msg_buf[-1] != 0:
            raise ProtocolError("Invalid command complete message")
        if self._result is None:
            raise ProtocolError("Unexpected close message.")
        command_tag = decode(msg_buf[:-1])
        if command_tag in ("DISCARD ALL", "DEALLOCATE ALL"):
            self._cache.clear()
        self._result.append((
            self.res_fields, self.res_rows, command_tag))
        self.res_fields = None
        self.res_converters = None
        self.res_rows = None

    def _handle_ready_cache(self) -> None:

        if self.cache_key is None:
            return

        cache_item = self._cache_item
        if cache_item is None:
            # Statement does not exist in cache
            if (self._ex is None and len(self._result) == 1 and
                    self._result[0][2] not in (
                        "DISCARD ALL", "DEALLOCATE ALL")):
                # Successful execution, new item must be added to cache.
                cache_len = len(self._cache)
                if cache_len == self._cache_size:
                    # Cache is full. Remove old statement, reuse statement
                    # name and close old statement if prepared
                    old_item = self._cache.popitem(last=False)[1]
                    stmt_name = old_item["name"]
                    if old_item["prepared"]:
                        self._stmt_to_close = old_item
                else:
                    # Cache not full yet, generate statement name
                    stmt_name = f"_pagio_{cache_len:03}".encode()
                # Add new cache item
                self._cache[self.cache_key] = {
                    "prepared": False,
                    "num_executed": 1,
                    "res_fields": None,
                    "bin_converters": None,
                    "txt_converters": None,
                    "name": stmt_name,
                }
        else:
            # Statement is already in cache
            if self._ex is None:
                # Successful execution, move to recent end in cache and
                # increment execution counter if not prepared yet
                self._cache.move_to_end(self.cache_key)
                if not cache_item["prepared"]:
                    cache_item["num_executed"] += 1
            else:
                # Error occurred
                if cache_item["prepared"]:
                    if self._ex.code == "26000":
                        cache_item.update(
                            prepared=False, num_executed=0, res_fields=None,
                            bin_converters=None, txt_converters=None)
                    else:
                        # Statement is server side prepared, mark for closure
                        self._stmt_to_close = cache_item
            self._cache_item = None
        self.cache_key = None

    def handle_ready_for_query(self, msg_buf: memoryview) -> None:
        """ Handles a Ready for Query message. """
        self._transaction_status = single_byte_struct_unpack(msg_buf)[0]
        self._status = _STATUS_READY_FOR_QUERY

        if self._prepare_threshold:
            self._handle_ready_cache()

        if self._ex is not None:
            self._set_exception(self._ex)
            self._ex = None
        else:
            self._set_result(self._result)
        self._result = None

    def txt_date_to_python(self, buf: memoryview) -> Union[str, date]:
        """ Converts PG textual date value to a Python date if possible. """
        if self._iso_dates:
            return txt_date_to_python(buf)
        return decode(buf)

    def txt_timestamp_to_python(self, buf: memoryview) -> Union[str, datetime]:
        """ Converts PG textual timestamp value to a Python datetime if
        possible.

        """
        if self._iso_dates:
            return txt_timestamp_to_python(buf)
        return decode(buf)

    def txt_timestamptz_to_python(
            self, buf: memoryview) -> Union[str, datetime]:
        """ Converts PG textual timestamptz value to a Python datetime with
        tzinfo if possible.

        """
        if self._iso_dates:
            return txt_timestamptz_to_python(buf)
        return decode(buf)

    def bin_timestamptz_to_python(
            self, buf: memoryview) -> Union[str, datetime]:
        """ Converts PG binary timestamptz value to a Python datetime with
        tzinfo if possible.

        """
        return bin_timestamptz_to_python(buf, self._tz_info)
