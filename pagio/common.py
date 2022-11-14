""" Common functionality """

from collections import namedtuple
import enum
from struct import Struct
from typing import Tuple, Any, Optional, Union, List, Iterator, NamedTuple


ushort_struct_unpack_from = Struct('!H').unpack_from


class Format(enum.IntEnum):
    """ Format used for data values """
    DEFAULT = -1
    TEXT = 0
    BINARY = 1


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


class ServerError(Error):
    """ Error reported by PostgreSQL server """

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


class CachedQueryExpired(ServerError):
    """ Error raised when a cached query is expired. """


class StatementDoesNotExist(ServerError):
    """ Error raised when a cached query is deallocated. """


FieldInfo = namedtuple(
    "FieldInfo",
    ["field_name", "table_oid", "col_num", "type_oid", "type_size", "type_mod",
     "format"])


class Result(NamedTuple):
    """ Result of single executed statement. """
    fields: Optional[List[FieldInfo]]
    rows: Optional[List[Tuple[Any, ...]]]
    command_tag: str

    @property
    def records_affected(self) -> Optional[int]:
        """ The number of affected records. """
        parts = self.command_tag.rsplit(" ", 1)
        if len(parts) == 2:
            recs = parts[1]
            if recs.isdigit():
                return int(recs)
        return None


class ResultSet:
    """ Result of executed statement """

    def __init__(
            self,
            results: List[Tuple[
                Optional[List[FieldInfo]], Optional[List[Tuple[Any, ...]]], str
            ]],
    ) -> None:
        self._results: List[Result] = [
            Result(*res_args) for res_args in results]
        self._result_index = 0

    def _current(self) -> Result:
        return self._results[self._result_index]

    def __iter__(self) -> Iterator[Tuple[Any, ...]]:
        result = self._current()
        if result.rows is None:
            raise ValueError("Not a resultset")
        return iter(result.rows)

    def __len__(self) -> int:
        result = self._current()
        if result.rows is None:
            raise ValueError("Not a resultset")
        return len(result.rows)

    def __getitem__(self, key: int) -> Tuple[Any, ...]:
        result = self._current()
        if result.rows is None:
            raise ValueError("Not a resultset")
        return result.rows[key]

    def next_result(self) -> bool:
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
    def fields(self) -> Optional[List[FieldInfo]]:
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
