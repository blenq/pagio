""" Base protocol functionality """

from abc import abstractmethod, ABC
from codecs import decode
from collections import OrderedDict
import enum
from hashlib import md5
from itertools import repeat
from struct import Struct, unpack_from, pack
import sys
from typing import (
    Optional, Union, Dict, Callable, List, Any, Tuple, cast, Generator,
    Type, OrderedDict as TypingOrderedDict, Iterable)
import warnings

from .pgscramp import PGScrampClient

from .types import default_res_converters, res_converters, param_converters
from .types.array import ArrayConverter, BinArrayConverter
from .common import (
    ProtocolError, Severity, _error_fields, ServerError, InvalidOperationError,
    FieldInfo, CachedQueryExpired, check_length_equal,
    ushort_struct, Format, StatementDoesNotExist, CopyFile,
    Notification, ResConverter, error_classes, ServerWarning, ServerNotice,
)
from .types import text
from .zoneinfo import ZoneInfo, ZoneInfoNotFoundError

if sys.version_info >= (3, 8):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict

warnings.filterwarnings("ignore", category=ServerNotice)

STANDARD_BUF_SIZE = 0x4000


msg_header_struct = Struct("!Bi")
int_struct = Struct('!i')
field_desc_struct = Struct("!IhIhih")
intint_struct = Struct('!ii')


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
    """ Unknown """
    IDLE = ord('I')
    """ No transaction in progress """
    TRANSACTION = ord('T')
    """ Transaction in progress """
    ERROR = ord('E')
    """ Transaction in error state """


class _AbstractPGProtocol(ABC):
    _prepare_threshold: int
    _cache_size: int
    _extended_query: bool
    _ex: Optional[Exception]
    file_obj: Optional[CopyFile]
    _custom_res_converters: Dict

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
        file_obj: Optional[CopyFile],
    ) -> List[bytes]:
        """ Executes a statement. """

    @abstractmethod
    def handle_message(self, identifier: int, buf: memoryview) -> None:
        """ Handle a received message """

    @abstractmethod
    def handle_copy_in_response(self, msg_buf: memoryview) -> None:
        """ Handle a copy in response """

    @abstractmethod
    def handle_copy_out_response(self, msg_buf: memoryview) -> None:
        """ Handle a copy out response """

    @abstractmethod
    def handle_copy_data_response(self, msg_buf: memoryview) -> None:
        """ Handle a copy data response """

    @abstractmethod
    def handle_copy_done_response(self, msg_buf: memoryview) -> None:
        """ Handle a copy done response """

    @abstractmethod
    def _setup_ssl_request(self) -> None:
        ...

    @abstractmethod
    def get_channel_binding(self) -> Optional[Tuple[str, bytes]]:
        """ Gets the channel binding for SASL authentication """

    @abstractmethod
    def enqueue_notification(self, notification: Notification) -> None:
        """ Puts a notification in the queue. """


CANCEL_REQUEST_CODE = 80877102


# pylint: disable-next=too-many-instance-attributes
class _BasePGProtocol(_AbstractPGProtocol):
    """ Common functionality for pure python and c accelerated versions of
    the PG protocol class, sync and async.

    """
    _transaction_status: int
    _status: int
    _server_parameters: Dict[str, str]
    _tzinfo: Optional[ZoneInfo]

    def __init__(self) -> None:
        self._handlers: Dict[int, Callable[[memoryview], None]] = {
            ord(k): v for k, v in [
                (' ', self.handle_ssl_response),
                ('E', self.handle_error),
                ('R', self.handle_auth_req),
                ('K', self.handle_backend_key_data),
                ('I', self.handle_empty_query_response),
                ('G', self.handle_copy_in_response),
                ('H', self.handle_copy_out_response),
                ('d', self.handle_copy_data_response),
                ('c', self.handle_copy_done_response),
                ('A', self.handle_notification_response),
                ('N', self.handle_notice_response),
            ]}
        self._backend: Optional[Tuple[int, int]] = None
        self.password: Union[None, bytes] = None
        self.user: Union[None, bytes] = None
        self.scram_client: Optional[PGScrampClient] = None
        self._ssl_in_use = False

    def register_res_converter(
            self,
            type_oid: int,
            txt_conv: ResConverter,
            res_conv: ResConverter,
            array_oid: int,
            delim: str
    ) -> None:
        self._custom_res_converters[type_oid] = (txt_conv, res_conv)
        if array_oid:
            self._custom_res_converters[array_oid] = (
                ArrayConverter(delim, txt_conv),
                BinArrayConverter(type_oid, res_conv),
            )

    def custom_res_conv(self, buf: memoryview, oid: int, fmt: Format) -> Any:
        return self._custom_res_converters[oid][fmt](self, buf)

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
    def tzinfo(self) -> Union[None, ZoneInfo]:
        """ Session timezone """
        return self._tzinfo

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
            password: Union[None, str, bytes],
            options: Optional[Dict[str, Optional[str]]],
            prepare_threshold: int, cache_size: int,
    ) -> bytes:
        parameters = []
        struct_format = ["!ii"]

        if isinstance(user, str):
            user = user.encode()

        if options is not None:
            options = {k.lower(): v for k, v in options.items()}
        else:
            options = {}
        options.update({
            "user": user,
            "database": database,
            "application_name": application_name,
            "timezone": tz_name,
            "DateStyle": "ISO",
            "client_encoding": "UTF8",
        })
        for name, value in options.items():

            if not value:
                continue
            bname = name.encode()
            if isinstance(value, str):
                value = value.encode()
            struct_format.append(f"{len(bname) + 1}s{len(value) + 1}s")
            parameters.extend([bname, value])

        # Add terminating zero
        struct_format.append("1s")
        parameters.append(b"")

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

    def _error_args(self, buf: memoryview) -> List[Any]:
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
        return ex_args

    def handle_error(self, buf: memoryview) -> None:
        """ Interprets and sets a server error. """

        ex_args = self._error_args(buf)

        if ex_args[16] == "RevalidateCachedQuery":
            # recognize this particular error, to easily handle retry
            ex_class: Type[ServerError] = CachedQueryExpired
        elif ex_args[1] == "26000":
            # recognize this particular error, to easily handle retry
            ex_class = StatementDoesNotExist
        else:
            ex_class = error_classes.get(ex_args[1][:2], ServerError)
        exc = ex_class(*ex_args)

        if ex_args[0] is Severity.FATAL or ex_args[0] is Severity.PANIC:
            # raise exc
            # self._close()
            self._set_exception(exc)
        elif self._ex is None:
            # non fatal and connected, raise when ReadyForQuery arrives
            self._ex = exc

    def handle_notice_response(self, buf: memoryview) -> None:
        ex_args = self._error_args(buf)
        if ex_args[0] is Severity.NOTICE:
            ex_class = ServerNotice
        else:
            ex_class = ServerWarning
        warnings.warn(ex_class(*ex_args))

    def _handle_md5_auth_req(self, msg_buf: memoryview) -> None:
        check_length_equal(8, msg_buf)
        if self.password is None:
            raise ProtocolError("Missing password")
        if self.user is None:
            raise ProtocolError("Missing user")
        [salt] = unpack_from("4s", msg_buf, 4)
        pw_hash = (b'md5' + md5(md5(
            self.password + self.user
        ).hexdigest().encode() + salt).hexdigest().encode())

        pw_len = len(pw_hash) + 1
        struct_fmt = f'!ci{pw_len}s'
        self._set_result(
            pack(struct_fmt, b'p', pw_len + 4, pw_hash))

    def handle_auth_req(self, msg_buf: memoryview) -> None:
        """ Handles authentication messages """
        [specifier] = int_struct.unpack_from(msg_buf)
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

    def handle_notification_response(self, msg_buf: memoryview) -> None:
        """ Handles a notification """
        if len(msg_buf) < 6 or msg_buf[-1] != 0:
            raise ProtocolError("Invalid notification reponse")
        [process_id] = int_struct.unpack_from(msg_buf)
        value = decode(msg_buf[4:-1])
        parts = value.split('\0')
        if len(parts) != 2:
            raise ProtocolError("Invalid notification reponse")
        channel, payload = parts
        self.enqueue_notification(Notification(
            process_id=process_id, channel=channel, payload=payload))

    def handle_empty_query_response(self, msg_buf: memoryview) -> None:
        """ Handles an empty query response. """
        check_length_equal(0, msg_buf)


CacheKey = Union[str, Tuple[str, Tuple[int]]]


class Statement(TypedDict):
    """ Statement cache item """
    prepared: bool
    num_executed: int
    res_fields: Optional[Tuple[FieldInfo, ...]]
    res_converters: Optional[List[Tuple[ResConverter, ResConverter]]]
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
        self._stmt_to_close: Optional[Statement] = None

        # reading buffers and counters
        self._bytes_read = 0
        self._buf = self._standard_buf = memoryview(
            bytearray(STANDARD_BUF_SIZE))
        self._msg_part_len = 5
        self._identifier: Optional[int] = None

        # resultset vars
        self.res_rows: Optional[List[Tuple[Any, ...]]] = None
        self.res_fields: Optional[Tuple[FieldInfo, ...]] = None
        self.res_converters: Optional[
            List[Tuple[ResConverter, ResConverter]]] = None
        self._result_format = Format.DEFAULT
        self._raw_result = False
        self._extended_query = False
        self.file_obj = None

        # return values
        self._result: Optional[List[Tuple[
            Optional[Tuple[FieldInfo, ...]],
            Optional[List[Tuple[Any, ...]]],
            str,
        ]]] = None
        self._ex = None

        # status vars
        self._status = _STATUS_CLOSED
        self._transaction_status = 0
        self._server_parameters: Dict[str, str] = {}
        self._iso_dates = False
        self._tzinfo: Optional[ZoneInfo] = None

        super().__init__(*args)
        self._handlers.update({
            ord(k): v for k, v in [
                ('S', self.handle_parameter_status),
                ('1', self.handle_parse_complete),
                ('2', self.handle_bind_complete),
                ('3', self.handle_close_complete),
                ('T', self.handle_row_description),
                ('n', self.handle_nodata),
                ('D', self.handle_data_row),
                ('C', self.handle_command_complete),
                ('Z', self.handle_ready_for_query),
            ]})
        self._custom_res_converters = {}
        self._interval_style = None

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
                self._identifier, msg_len = msg_header_struct.unpack_from(
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
        return param_converters.get(type(param), text.default_to_pg)(param)

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
                        if self.res_fields is not None:
                            self.res_converters = cache_item["res_converters"]
                            self.res_rows = []
                else:
                    if cache_item["num_executed"] == self._prepare_threshold:
                        # Using a non-empty statement name for reuse
                        stmt_name = cache_item["name"]
        if result_format == Format.DEFAULT:
            if not param_oids and not prepared and not stmt_name:
                result_format = Format.TEXT
            else:
                result_format = Format.BINARY
        return stmt_name, prepared, result_format

    def execute_message(
            self,
            sql: str,
            parameters: Tuple[Any, ...],
            result_format: Format,
            raw_result: bool,
            file_obj: Optional[CopyFile],
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
                self._tzinfo = ZoneInfo(val)
            except ZoneInfoNotFoundError:
                self._tzinfo = None
        elif name == "IntervalStyle":
            self._interval_style = val
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
        self._stmt_to_close.update({
            "prepared": False, "num_executed": 0, "res_fields": None,
            "res_converters": None,
        })
        self._stmt_to_close = None

    def handle_nodata(self, msg_buf: memoryview) -> None:
        """ Handles a nodata message """
        check_length_equal(0, msg_buf)

    def handle_row_description(self, msg_buf: memoryview) -> None:
        """ Handles a Row Description message. """
        buffer = bytes(msg_buf)
        res_fields = []
        converters: List[Tuple[ResConverter, ResConverter]] = []
        [num_fields] = ushort_struct.unpack_from(msg_buf)

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
                field_desc_struct.unpack_from(msg_buf, offset))
            res_fields.append(FieldInfo(
                field_name, table_oid, col_num, type_oid, type_size, type_mod,
                _format))
            convs = self._custom_res_converters.get(type_oid)
            if convs is None:
                convs = res_converters.get(type_oid, default_res_converters)
            converters.append(convs)
            offset += field_desc_struct.size
        if offset != len(msg_buf):
            raise ProtocolError("Additional data after row description")
        self.res_fields = (*res_fields,)
        self.res_rows = []
        self.res_converters = converters
        if self._cache_item is not None and self._cache_item["prepared"]:
            # store field_info and converters in cache
            self._cache_item["res_converters"] = converters
            self._cache_item["res_fields"] = self.res_fields

    def handle_data_row(self, buf: memoryview) -> None:
        """ Handles a DataRow message. """
        if self.res_converters is None or self.res_rows is None:
            raise ProtocolError("Unexpected data row.")

        num_converters = len(self.res_converters)
        if ushort_struct.unpack_from(buf)[0] != num_converters:
            raise ProtocolError("Invalid number of row values")

        res_converters: Iterable[Callable[[memoryview], Any]]
        if self._raw_result:
            res_converters = repeat(
                default_res_converters[self._result_format], num_converters)
        else:
            res_converters = (
                convs[self._result_format] for convs in self.res_converters)

        def get_vals() -> Generator[Any, None, None]:
            offset = 2
            for converter in res_converters:
                [val_len] = int_struct.unpack_from(buf, offset)
                offset += 4
                if val_len == -1:
                    yield None
                elif val_len < 0:
                    raise ProtocolError("Negative length value")
                else:
                    val = converter(self, buf[offset:offset + val_len])
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

    # pylint: disable-next=too-many-branches
    def _handle_ready_cache(self) -> None:

        if self.cache_key is None:
            return

        if self._result is None:
            raise ProtocolError("Unexpected ReadyForQuery")

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
                    "res_converters": None,
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
                    if (isinstance(self._ex, ServerError) and
                            self._ex.code == "26000"):
                        # statement does not exist on the server anymore
                        cache_item.update({
                            "prepared": False, "num_executed": 0,
                            "res_fields": None, "res_converters": None,
                        })
                    else:
                        # Statement is server side prepared, mark for closure
                        self._stmt_to_close = cache_item
            self._cache_item = None
        self.cache_key = None

    def handle_ready_for_query(self, msg_buf: memoryview) -> None:
        """ Handles a Ready for Query message. """
        self._transaction_status = msg_buf[0]
        self._status = _STATUS_READY_FOR_QUERY

        if self._prepare_threshold:
            self._handle_ready_cache()

        if self._ex is not None:
            self._set_exception(self._ex)
            self._ex = None
        else:
            self._set_result(self._result)
        self._result = None
