""" Common functionality """

from collections import namedtuple
import enum
from typing import Tuple, Any, Optional, Union, List, Iterator


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
    pass


class ProtocolError(Error):
    pass


class InvalidOperationError(Error):
    pass


class ServerError(Error):

    args: Tuple[
        Severity, str, str, Optional[str], Optional[str],
        Union[None, str, int], Union[None, str, int], Optional[str],
        Optional[str], Optional[str], Optional[str], Optional[str],
        Optional[str], Optional[str], Optional[str], Union[None, str, int],
        Optional[str]]

    @property
    def severity(self) -> Severity:
        return self.args[0]

    @property
    def code(self) -> str:
        return self.args[1]

    @property
    def message(self) -> str:
        return self.args[2]

    @property
    def detail(self) -> Optional[str]:
        return self.args[3]

    @property
    def hint(self) -> Optional[str]:
        return self.args[4]

    @property
    def position(self) -> Union[None, str, int]:
        return self.args[5]

    @property
    def internal_position(self) -> Union[None, str, int]:
        return self.args[6]

    @property
    def internal_query(self) -> Optional[str]:
        return self.args[7]

    @property
    def where(self) -> Optional[str]:
        return self.args[8]

    @property
    def schema_name(self) -> Optional[str]:
        return self.args[9]

    @property
    def table_name(self) -> Optional[str]:
        return self.args[10]

    @property
    def column_name(self) -> Optional[str]:
        return self.args[11]

    @property
    def data_type_name(self) -> Optional[str]:
        return self.args[12]

    @property
    def constraint_name(self) -> Optional[str]:
        return self.args[13]

    @property
    def file_name(self) -> Optional[str]:
        return self.args[14]

    @property
    def line_number(self) -> Union[None, str, int]:
        return self.args[15]

    @property
    def routine_name(self) -> Optional[str]:
        return self.args[16]

    def __str__(self) -> str:
        return str(self.args[:3])


class CachedQueryExpired(ServerError):
    pass


FieldInfo = namedtuple(
    "FieldInfo",
    ["field_name", "table_oid", "col_num", "type_oid", "type_size", "type_mod",
     "format"])


class Result:

    def __init__(
            self,
            fields: Optional[List[FieldInfo]],
            rows: Optional[List[Tuple[Any, ...]]],
            command_tag: str):
        self.fields = fields
        self.rows = rows
        self.command_tag = command_tag


class ResultSet:

    def __init__(self, results) -> None:
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
        num_results = len(self._results)
        if self._result_index < num_results:
            self._result_index += 1
        return self._result_index < num_results

    @property
    def command_tag(self) -> str:
        return self._current().command_tag

    @property
    def fields(self) -> Optional[List[str]]:
        return self._current().fields

    @property
    def rows(self) -> Optional[List[Tuple[Any, ...]]]:
        return self._current().rows

    def __bool__(self):
        return bool(self._results)
