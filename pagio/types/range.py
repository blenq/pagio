from abc import ABC, abstractmethod
from enum import Flag
from functools import partial, cached_property
import operator
import struct
from typing import Generic, Optional, TypeVar, Type, Callable, Union

from .array import parse_unquoted, parse_quoted, quote
from ..common import ResConverter

T = TypeVar('T')


class BasePGRange(Generic[T]):

    def __init__(
            self,
            lower: Optional[Union[str, T]],
            upper: Optional[T],
            bounds: Optional[str] = '[)'
    ):

        if bounds is None:
            if lower is not None or upper is not None:
                raise ValueError(
                    "If bounds is None, lower and upper bound can not be set.")
            self._set_empty()
            return

        if isinstance(lower, str):
            if lower != 'empty':
                raise ValueError("Invalid value for lower bound.")
            if upper is not None:
                raise  ValueError(
                    "If 'empty' value, upper bound can not be set.")
            self._set_empty()
            return

        self._empty = False
        if (len(bounds) != 2 or bounds[0] not in ['[', '('] or
                bounds[1] not in [')', ']']):
            raise ValueError("Invalid bounds")

        if lower is None:
            self._lower_inc = False
        else:
            self._lower_inc = bounds[0] == '['
            lower = self._type_check(lower)

        if upper is None:
            self._upper_inc = False
        else:
            self._upper_inc = bounds[1] == ']'
            upper = self._type_check(upper)

        if upper is not None and lower is not None:
            if upper < lower:
                raise ValueError(
                    "Lower bound must be less than or equal to upper bound.")
            if upper == lower and (not self._lower_inc or not self._upper_inc):
                # Same as postgres. For example: SELECT '[10, 10)'::int4range
                self._set_empty()
                return

        self._lower = lower
        self._upper = upper

    @property
    def lower(self):
        """ Lower bound """
        return self._lower

    @property
    def upper(self):
        """ Upper bound """
        return self._upper

    @property
    def bounds(self):
        """ Bounds """
        if self._empty:
            return None
        return f"{['(', '['][self._lower_inc]}{[')', ']'][self._upper_inc]}"

    def _set_empty(self):
        self._lower = None
        self._upper = None
        self._lower_inc = False
        self._upper_inc = False
        self._empty = True

    @classmethod
    def empty(cls) -> 'BaseRange[T]':
        """ Returns an empty range """
        return cls(None, None, None)

    @property
    def is_empty(self):
        return self._empty

    @cached_property
    def _contains_checks(self):
        checks = []

        if self._lower is not None:
            if self._lower_inc:
                lower_check = partial(operator.le, self._lower)
            else:
                lower_check = partial(operator.lt, self._lower)
            checks.append(lower_check)

        if self._upper is not None:
            if self._upper_inc:
                upper_check = partial(operator.ge, self._upper)
            else:
                upper_check = partial(operator.gt, self._upper)
            checks.append(upper_check)

        return checks

    def __contains__(self, item: T) -> bool:
        item = self._type_check(item)
        if self._empty:
            return False
        return all(check(item) for check in self._contains_checks)

    def __eq__(self, other):
        return (
            isinstance(other, BasePGRange) and self._lower == other._lower and
            self._upper == other._upper and
            self._lower_inc == other._lower_inc and
            self._upper_inc == other._upper_inc)

    def _type_check(self, val: T) -> T:
        return val

    def __str__(self):
        if self._empty:
            return "empty"
        start = '[' if self._lower_inc else '('
        end = ']' if self._upper_inc else ')'
        return f"{start}{self._lower},{self._upper}{end}"

    def __repr__(self):
        if self._empty:
            bounds = None
        else:
            bounds = ("'[" if self._lower_inc else "'(") + (
                "]'" if self._upper_inc else ")'")
        return (
            f"{self.__class__.__name__}({self._lower}, {self._upper}, "
            f"{bounds})")


class DiscreteRange(BasePGRange[T], ABC):

    def __init__(
            self,
            lower: Optional[T],
            upper: Optional[T],
            bounds: Optional[str] = '[)'
    ):
        super().__init__(lower, upper, bounds)
        if self._lower is not None and not self._lower_inc:
            self._lower = self._type_check(self.increment(self._lower))
            self._lower_inc = True
        if self._upper is not None and self._upper_inc:
            self._upper = self._type_check(self.increment(self._upper))
            self._upper_inc = False

    @abstractmethod
    def increment(self, value: T) -> T:
        """ Increments value """


class RangeFlags(Flag):
    RANGE_EMPTY = 0x01
    RANGE_LB_INC = 0x02
    RANGE_UB_INC = 0x04
    RANGE_LB_INF = 0x08
    RANGE_UB_INF = 0x10
    RANGE_LB_NULL = 0x20  # lower bound is null (NOT USED)
    RANGE_UB_NULL = 0x40  # upper bound is null (NOT USED)
    RANGE_CONTAIN_EMPTY = 0x80  # Something internal to postgres and GiST


int4_struct = struct.Struct("!i")


class BinRangeResultConverter(Generic[T]):

    def __init__(self, range_class: Type[BasePGRange[T]], conv: ResConverter):
        self.range_class = range_class
        self.conv = conv

    def __call__(self, conn, buf: memoryview) -> BasePGRange[T]:
        flags = RangeFlags(buf[0])
        pos = 1
        if flags & RangeFlags.RANGE_EMPTY:
            lower = None
            upper = None
            bounds = None
        else:
            if flags & RangeFlags.RANGE_LB_INC:
                lb_char = '['
            else:
                lb_char = '('
            if flags & RangeFlags.RANGE_UB_INC:
                ub_char = ']'
            else:
                ub_char = ')'
            bounds = lb_char + ub_char
            if flags & RangeFlags.RANGE_LB_INF:
                lower = None
            else:
                [lower_len] = int4_struct.unpack_from(buf, pos)
                pos += 4
                lower = self.conv(conn, buf[pos:pos + lower_len])
                pos += lower_len
            if flags & RangeFlags.RANGE_UB_INF:
                upper = None
            else:
                [upper_len] = int4_struct.unpack_from(buf, pos)
                pos += 4
                upper = self.conv(conn, buf[pos:pos + upper_len])
                pos += upper_len
        if pos != len(buf):
            raise ValueError("Invalid binary range value")
        return self.range_class(lower, upper, bounds)


comma = ord(',')
range_end = (ord(']'), ord(')'))


class TxtRangeResultConverter(Generic[T]):

    def __init__(self, range_class: Type[BasePGRange[T]], conv: ResConverter):
        self.range_class = range_class
        self.conv = conv

    def __call__(self, conn, buf: memoryview) -> BasePGRange[T]:
        if buf == b'empty':
            lower = None
            upper = None
            bounds = None
        else:
            pos = 1
            if buf[pos] == quote:
                lower, new_pos = parse_quoted(buf[pos:], conn, self.conv)
                pos += new_pos
            elif buf[pos] == comma:
                lower = None
            else:
                lower, new_pos = parse_unquoted(
                    buf[pos:], (comma,), conn, self.conv)
                pos += new_pos
            if buf[pos] != comma:
                raise ValueError("Invalid range value.")
            pos += 1
            if buf[pos] == quote:
                upper, new_pos = parse_quoted(buf[pos:], conn, self.conv)
                pos += new_pos
            elif buf[pos] in range_end:
                upper = None
            else:
                upper, new_pos = parse_unquoted(
                    buf[pos:], range_end, conn, self.conv)
                pos += new_pos

            bounds = chr(buf[0]) + chr(buf[pos])
            pos += 1

            if pos != len(buf):
                raise ValueError("Invalid textual range value.")
        return self.range_class(lower, upper, bounds)
