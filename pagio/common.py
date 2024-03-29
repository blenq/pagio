""" Common functionality """

import enum
from functools import partial
from typing import (
    Tuple, Any, Optional, Union, List, Iterator, NamedTuple, Callable)

try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol  # type: ignore

int_from_bytes = partial(int.from_bytes, byteorder="big", signed=True)
uint_from_bytes = partial(int.from_bytes, byteorder="big")


def int4_to_bytes(val: int) -> bytes:
    """ Convert int to 4 byte signed bigendian value. """
    return val.to_bytes(4, "big", signed=True)


class Format(enum.IntEnum):
    """ Format used for data values """
    DEFAULT = -1
    TEXT = 0
    BINARY = 1


ParamConverter = Callable[[Any], Tuple[int, str, Any, int, Format]]


class Severity(enum.Enum):
    """ Severity of ServerError """
    ERROR = 'ERROR'
    FATAL = 'FATAL'
    PANIC = 'PANIC'
    WARNING = 'WARNING'
    NOTICE = 'NOTICE'
    DEBUG = 'DEBUG'
    INFO = 'INFO'
    LOG = 'LOG'


_error_fields = {
    "C": 1,
    "M": 2,
    "D": 3,
    "H": 4,
    "P": 5,
    "p": 6,
    "q": 7,
    "w": 8,
    "s": 9,
    "t": 10,
    "c": 11,
    "d": 12,
    "n": 13,
    "F": 14,
    "L": 15,
    "R": 16,
}


class Error(Exception):
    """ Pagio error """


class ProtocolError(Error):
    """ Pagio error caused by parsing invalid content by pagio library. """


class InvalidOperationError(Error):
    """ Invalid operation error """


class InterfaceError(Error):
    """ Interface error """


class ServerInfoMixin:
    """ Mixin for PG server exception/notice arguments. """
    args: Tuple[
        Severity, str, str, Optional[str], Optional[str],
        Union[None, str, int], Union[None, str, int], Optional[str],
        Optional[str], Optional[str], Optional[str], Optional[str],
        Optional[str], Optional[str], Optional[str], Union[None, str, int],
        Optional[str]]

    @property
    def severity(self) -> Severity:
        """ Severity of server error."""
        return self.args[0]

    @property
    def code(self) -> str:
        """ Error code """
        return self.args[1]

    @property
    def message(self) -> str:
        """ Error message """
        return self.args[2]

    @property
    def detail(self) -> Optional[str]:
        """ Detailed error message """
        return self.args[3]

    @property
    def hint(self) -> Optional[str]:
        """ Hint to solve error """
        return self.args[4]

    @property
    def position(self) -> Union[None, str, int]:
        """ Position where error occurred """
        return self.args[5]

    @property
    def internal_position(self) -> Union[None, str, int]:
        """ Internal position where error occurred """
        return self.args[6]

    @property
    def internal_query(self) -> Optional[str]:
        """ Internal query where error occurred """
        return self.args[7]

    @property
    def where(self) -> Optional[str]:
        """ WHere """
        return self.args[8]

    @property
    def schema_name(self) -> Optional[str]:
        """ Schema name that applies to error. """
        return self.args[9]

    @property
    def table_name(self) -> Optional[str]:
        """ Table name that applies to error."""
        return self.args[10]

    @property
    def column_name(self) -> Optional[str]:
        """ Column name that applies to error. """
        return self.args[11]

    @property
    def data_type_name(self) -> Optional[str]:
        """ Data type name that error applies to. """
        return self.args[12]

    @property
    def constraint_name(self) -> Optional[str]:
        """ Constraint name that error applies to."""
        return self.args[13]

    @property
    def file_name(self) -> Optional[str]:
        """ File name that error raising code contains. """
        return self.args[14]

    @property
    def line_number(self) -> Union[None, str, int]:
        """ Line number of error raising code. """
        return self.args[15]

    @property
    def routine_name(self) -> Optional[str]:
        """ Routine name of error raising code. """
        return self.args[16]

    def __str__(self) -> str:
        return str(self.args[:3])


class ServerError(ServerInfoMixin, Error):  # type: ignore
    """ Error reported by PostgreSQL server """


class DataError(ServerError):
    """ Data Error """


class OperationalError(ServerError):
    """ Data Error """


class IntegrityError(ServerError):
    """ Data Error """


class InternalError(ServerError):
    """ Data Error """


class ProgrammingError(ServerError):
    """ Data Error """


class NotSupportedError(ServerError):
    """ Not suppored """


class StatementDoesNotExist(InternalError):
    """ Error raised when a cached query is deallocated. """


class CachedQueryExpired(InternalError):
    """ Error raised when a cached query is expired. """


class ServerWarning(ServerInfoMixin, Warning):  # type: ignore
    """ Warning class """


class ServerNotice(ServerInfoMixin, Warning):  # type: ignore
    """ Warning class """


error_classes = {
    "08": OperationalError,
    "0A": NotSupportedError,
    "0Z": InternalError,
    "20": ProgrammingError,
    "21": ProgrammingError,
    "22": DataError,
    "23": IntegrityError,
    "24": InternalError,
    "25": InternalError,
    "26": InternalError,
    "27": InternalError,
    "28": OperationalError,
    "2B": InternalError,
    "2D": InternalError,
    "2F": InternalError,
    "34": ProgrammingError,
    "38": InternalError,
    "39": InternalError,
    "3B": InternalError,
    "3D": ProgrammingError,
    "3F": ProgrammingError,
    "40": OperationalError,
    "42": ProgrammingError,
    "44": IntegrityError,
    "53": OperationalError,
    "54": OperationalError,
    "55": OperationalError,
    "57": OperationalError,
    "58": OperationalError,
    "72": InternalError,
    "F0": InternalError,
    "HV": OperationalError,
    "P0": InternalError,
    "XX": InternalError,
}


class FieldInfo(NamedTuple):
    """ Field information """
    field_name: str
    table_oid: int
    col_num: int
    type_oid: int
    type_size: int
    type_mod: int
    format: int


class Result:
    """ Result of single executed statement. """

    def __init__(
            self,
            fields: Optional[Tuple[FieldInfo, ...]],
            rows: Optional[List[Tuple[Any, ...]]],
            command_tag: str,
    ) -> None:
        self.fields = fields
        self.rows = rows
        self.command_tag = command_tag

    @property
    def records_affected(self) -> Optional[int]:
        """ The number of affected records. """
        parts = self.command_tag.rsplit(" ", 1)
        if len(parts) == 2:
            recs = parts[1]
            if recs.isdigit():
                return int(recs)
        return None

    def _row_list(self) -> List[Tuple[Any, ...]]:
        if self.rows is None:
            raise ValueError("Not a row returning statement.")
        return self.rows

    def __iter__(self) -> Iterator[Tuple[Any, ...]]:
        return iter(self._row_list())

    def __len__(self) -> int:
        return len(self._row_list())

    def __getitem__(self, key: int) -> Tuple[Any, ...]:
        return self._row_list()[key]


class ResultSet:
    """ Result of executed statement """

    def __init__(
            self,
            results: List[Tuple[
                Optional[Tuple[FieldInfo, ...]], Optional[List[Tuple[Any, ...]]], str
            ]],
    ) -> None:
        self._results: List[Result] = [
            Result(*res_args) for res_args in results]
        self._result_index = 0

    def _current(self) -> Result:
        try:
            return self._results[self._result_index]
        except IndexError as ex:
            raise ValueError("No more result sets available.") from ex

    def __iter__(self) -> Iterator[Tuple[Any, ...]]:
        return iter(self._current())

    def __len__(self) -> int:
        return len(self._current())

    def __getitem__(self, key: int) -> Tuple[Any, ...]:
        return self._current()[key]

    def nextset(self) -> bool:
        """ Advances to the next result set. """
        num_results = len(self._results)
        if self._result_index < num_results:
            self._result_index += 1
        return self._result_index < num_results

    @property
    def command_tag(self) -> str:
        """ The command tag as reported by the server. """
        return self._current().command_tag

    @property
    def records_affected(self) -> Optional[int]:
        """ The number of affected records. """
        return self._current().records_affected

    @property
    def fields(self) -> Optional[Tuple[FieldInfo, ...]]:
        """ List of fields """
        return self._current().fields

    @property
    def rows(self) -> Optional[List[Tuple[Any, ...]]]:
        """ List of rows """
        return self._current().rows

    def __bool__(self) -> bool:
        return bool(self._results)


def check_length_equal(length: int, msg_buf: memoryview) -> None:
    """ Checks for a fixed length """
    if len(msg_buf) != length:
        raise ProtocolError(
            f"Invalid length for message. Expected {length}, but got"
            f"{len(msg_buf)}.")


# pylint: disable-next=too-few-public-methods
class SyncCopyInputFile(Protocol):
    """ Type definition for synchronous input file. """
    def read(self, num: int) -> Union[bytes, str]:
        """ Read from the file """


# pylint: disable-next=too-few-public-methods
class AsyncCopyInputFile(Protocol):
    """ Type definition for ssynchronous input file. """
    async def read(self, num: int) -> Union[bytes, str]:
        """ Read from the file """


# pylint: disable-next=too-few-public-methods
class SyncCopyOutputFile(Protocol):
    """ Type definition for synchronous output file. """
    def write(self, data: bytes) -> int:
        """ Write data to the file """


# pylint: disable-next=too-few-public-methods
class AsyncCopyOutputFile(Protocol):
    """ Type definition for asynchronous output file. """
    async def write(self, data: bytes) -> int:
        """ Write data to the file """


SyncCopyFile = Union[SyncCopyInputFile, SyncCopyOutputFile]
CopyFile = Union[SyncCopyFile, AsyncCopyInputFile, AsyncCopyOutputFile]


class Notification(NamedTuple):
    """ Represents a PostgreSQL notification. """
    process_id: int
    channel: str
    payload: str
