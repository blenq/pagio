from abc import ABC, abstractmethod
from enum import Flag
from operator import attrgetter
import sys
from typing import (
    ClassVar, Generic, Optional, TypeVar, Type, Union, Any, Iterable, Set,
    Tuple, List,
)


if sys.version_info >= (3, 8):
    from typing import Protocol
else:
    from typing_extensions import Protocol

import pagio

from .array import parse_unquoted, parse_quoted, quote
from ..common import int_from_bytes
from .conv_utils import ResConverter, comma, right_parens


class SupportsLT(Protocol):
    def __lt__(self, other: 'T') -> bool:
        ...


T = TypeVar('T')


class RangeInfinity:

    _singleton: ClassVar[Optional['RangeInfinity']]

    def __new__(cls) -> 'RangeInfinity':
        if cls._singleton is None:
            cls._singleton = super().__new__(cls)
        return cls._singleton


class PositiveInfinity(RangeInfinity):

    _singleton = None

    def __lt__(self, other: Any) -> bool:
        return False

    def __le__(self, other: Any) -> bool:
        return self is other

    def __gt__(self, other: Any) -> bool:
        return self is not other

    def __ge__(self, other: Any) -> bool:
        return True


class NegativeInfinity(RangeInfinity):

    _singleton = None

    def __lt__(self, other: Any) -> bool:
        return self is not other

    def __le__(self, other: Any) -> bool:
        return True

    def __gt__(self, other: Any) -> bool:
        return False

    def __ge__(self, other: Any) -> bool:
        return self is other


class RangeBound(Generic[T]):

    def __init__(
            self,
            value: Optional[Union[T, RangeInfinity]],
            upper: bool, inclusive: bool,
    ) -> None:
        self.value = value
        self.rank = int(upper == inclusive)
        self.inclusive = inclusive

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RangeBound):
            return NotImplemented
        return (self.value, self.rank, self.inclusive) == (
            other.value, other.rank, self.inclusive)

    def __gt__(self, other: object) -> bool:
        return not self.__le__(other)  # type: ignore

    def __ge__(self, other: object) -> bool:
        return not self.__lt__(other)  # type: ignore

    def __str__(self) -> str:
        return str((self.value, self.rank))

    def __hash__(self) -> int:
        return hash((self.value, self.rank, self.inclusive))


class LowerBound(RangeBound[T]):

    def __init__(self, value: Optional[T], inclusive: bool) -> None:
        if value is None:
            act_value: Union[NegativeInfinity, T] = NegativeInfinity()
        else:
            act_value = value
        super().__init__(act_value, False, inclusive)

    def __lt__(self, other: Union[T, RangeBound[T]]) -> bool:
        if isinstance(other, RangeBound):
            other_rank = other.rank
            other_value = other.value
        else:
            other_rank = False
            other_value = other
        return (self.value, self.rank) < (other_value, other_rank)

    def __le__(self, other: Union[T, RangeBound[T]]) -> bool:
        if isinstance(other, RangeBound):
            other_rank = other.rank
            other_value = other.value
        else:
            other_rank = False
            other_value = other
        return (self.value, self.rank) <= (other_value, other_rank)


class UpperBound(RangeBound[T]):

    def __init__(self, value: Optional[T], inclusive: bool) -> None:
        if value is None:
            act_value: Union[T, PositiveInfinity] = PositiveInfinity()
        else:
            act_value = value
        super().__init__(act_value, True, inclusive)
        self.rank -= 1

    def __lt__(self, other: Union[T, RangeBound[T]]) -> bool:
        if isinstance(other, RangeBound):
            other_rank = other.rank
            other_value = other.value
        else:
            other_rank = True
            other_value = other
        return (self.value, self.rank) < (other_value, other_rank)

    def __le__(self, other: Union[T, RangeBound[T]]) -> bool:
        if isinstance(other, RangeBound):
            other_rank = other.rank
            other_value = other.value
        else:
            other_rank = True
            other_value = other
        return (self.value, self.rank) <= (other_value, other_rank)


class BasePGRange(ABC, Generic[T]):

    def __init__(
            self,
            lower: Optional[Union[str, T]],
            upper: Optional[T] = None,
            bounds: Optional[str] = '[)'
    ):
        self._bounds: Optional[Tuple[LowerBound[T], UpperBound[T]]] = None

        if bounds is None:
            if lower is not None or upper is not None:
                raise ValueError(
                    "If bounds is None, lower and upper bound can not be set.")
            return

        if isinstance(lower, str) and lower == 'empty':
            if upper is not None:
                raise ValueError(
                    "If 'empty' value, upper bound can not be set.")
            return

        if (len(bounds) != 2 or bounds[0] not in ['[', '('] or
                bounds[1] not in [')', ']']):
            raise ValueError("Invalid bounds")

        if lower is None:
            lower_inclusive = False
        else:
            lower_inclusive = bounds[0] == '['
            lower = self._type_check(lower)

        if upper is None:
            upper_inclusive = False
        else:
            upper_inclusive = bounds[1] == ']'
            upper = self._type_check(upper)

        if upper is not None and lower is not None:
            if upper < lower:
                raise ValueError(
                    "Lower bound must be less than or equal to upper bound.")
            if upper == lower and (not lower_inclusive or not upper_inclusive):
                # Same as postgres. For example: SELECT '[10, 10)'::int4range
                return
        self._bounds = (LowerBound(lower, lower_inclusive),
                        UpperBound(upper, upper_inclusive))

    @property
    def lower(self) -> Optional[T]:
        """ Lower bound """
        if self._bounds is None:
            return None
        lower_bound = self._bounds[0]
        if isinstance(lower_bound.value, RangeInfinity):
            return None
        return lower_bound.value

    @property
    def lower_inc(self) -> bool:
        """ Lower bound inclusive """
        return self._bounds is not None and self._bounds[0].inclusive

    @property
    def lower_inf(self) -> bool:
        return (
            self._bounds is not None and
            self._bounds[0].value is NegativeInfinity())

    @property
    def upper(self) -> Optional[T]:
        """ Upper bound """
        if self._bounds is None:
            return None
        upper_bound = self._bounds[1]
        if isinstance(upper_bound.value, RangeInfinity):
            return None
        return upper_bound.value

    @property
    def upper_inc(self) -> bool:
        """ Upper bound inclusive """
        return self._bounds is not None and self._bounds[1].inclusive

    @property
    def upper_inf(self) -> bool:
        return (
            self._bounds is not None and
            self._bounds[1].value is PositiveInfinity())

    @property
    def bounds(self) -> Optional[str]:
        """ Bounds """
        if self._bounds is None:
            return None
        return f"{['(', '['][self.lower_inc]}{[')', ']'][self.upper_inc]}"

    @classmethod
    def empty(cls) -> 'BasePGRange[T]':
        """ Returns an empty range """
        return cls(None, None, None)

    @property
    def is_empty(self) -> bool:
        return self._bounds is None

    def __contains__(self, item: Union[T, 'BasePGRange[T]']) -> bool:
        if isinstance(item, self.__class__):
            if item._bounds is None:
                return True
            if self._bounds is None:
                return False
            return (
                self._bounds[0] <= item._bounds[0] <= self._bounds[1] and
                self._bounds[0] <= item._bounds[1] <= self._bounds[1])
        item = self._type_check(item)
        if self._bounds is None:
            return False
        return self._bounds[0] <= item <= self._bounds[1]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self._bounds == other._bounds

    def __hash__(self) -> int:
        return hash(self._bounds)

    def overlaps(self, other: 'BasePGRange[T]') -> bool:
        if self._bounds is None or other._bounds is None:
            return False
        return (self._bounds[1] >= other._bounds[0] and
                self._bounds[0] <= other._bounds[1])

    def is_adjacent_to(self, other: 'BasePGRange[T]') -> bool:
        if self._bounds is None or other._bounds is None:
            return False
        return (
            self.lower == other.upper and
            self._bounds[0].inclusive + other._bounds[1].inclusive == 1
        ) or (
            self.upper == other.lower and
            self._bounds[1].inclusive + other._bounds[0].inclusive == 1)

    def __or__(self, other: 'BasePGRange[T]') -> 'BasePGRange[T]':
        if not isinstance(other, self.__class__):
            raise TypeError(f"Invalid type.")
        if self._bounds is None:
            return other
        if other._bounds is None:
            return self
        if self.overlaps(other) or self.is_adjacent_to(other):
            return self.merge(other)
        raise ValueError("Ranges do not overlap and are not adjacent.")

    def merge(self, other: 'BasePGRange[T]') -> 'BasePGRange[T]':
        if self._bounds is None:
            return other
        if other._bounds is None:
            return self
        lower_bound = min(self._bounds[0], other._bounds[0])
        if isinstance(lower_bound.value, RangeInfinity):
            lower_val = None
            lower_inc = '('
        else:
            lower_val = lower_bound.value
            lower_inc = '[' if lower_bound.inclusive else '('
        upper_bound = max(self._bounds[1], other._bounds[1])
        if isinstance(upper_bound.value, RangeInfinity):
            upper_val = None
            upper_inc = ')'
        else:
            upper_val = upper_bound.value
            upper_inc = ']' if upper_bound.inclusive else ')'
        return self.__class__(lower_val, upper_val, lower_inc + upper_inc)

    @abstractmethod
    def _type_check(self, val: Any) -> T:
        """ Needs to be overridden. """

    def __str__(self) -> str:
        if self._bounds is None:
            return "empty"
        lower = '' if self.lower is None else self.lower
        upper = '' if self.upper is None else self.upper
        return (
            f"{'[' if self.lower_inc else '('}{lower},"
            f"{upper}{']' if self.upper_inc else ')'}")

    def __repr__(self) -> str:
        if self._bounds is None:
            bounds = None
        else:
            bounds = ("[" if self.lower_inc else "(") + (
                "]" if self.upper_inc else ")")
        return (
            f"{self.__class__.__name__}({self.lower!r}, {self.upper!r}"
            f", {bounds!r})")


class DiscreteRange(BasePGRange[T], ABC):

    def __init__(
            self,
            lower: Optional[T],
            upper: Optional[T] = None,
            bounds: Optional[str] = '[)'
    ) -> None:
        super().__init__(lower, upper, bounds)
        # normalize to '[)' range
        if self.lower is not None and not self.lower_inc:
            self._bounds = (
                LowerBound(self._type_check(self.increment(self.lower)), True),
                self._bounds[1])
        if self.upper is not None and self.upper_inc:
            self._bounds = (
                self._bounds[0],
                UpperBound(
                    self._type_check(self.increment(self.upper)), False))

    @abstractmethod
    def increment(self, value: T) -> T:
        """ Increments value """


def _get_lower_bound(range_val: BasePGRange[T]) -> LowerBound[T]:
    return range_val._bounds[0]


def _normalize_ranges(
        ranges: Iterable[BasePGRange[T]]) -> List[BasePGRange[T]]:
    """ Normalizes ranges as an ordered list of non overlapping range objects.

    """
    ranges_to_add = list(ranges)
    non_overlapping: Set[BasePGRange[T]] = set()

    for range_to_add in ranges_to_add:
        for r in non_overlapping:
            if r.overlaps(range_to_add) or r.is_adjacent_to(range_to_add):
                # New range overlaps with a previous one. Remove previous one
                # and add the union of it with the new range to the process
                # list.
                non_overlapping.remove(r)
                ranges_to_add.append(r.merge(range_to_add))
                break
        else:
            # New range does not overlap with any of the already added ranges.
            # Add it to the non overlapping set.
            non_overlapping.add(range_to_add)

    # Sort the ranges and return
    return sorted(non_overlapping, key=_get_lower_bound)


class BaseMultiRange(Generic[T]):
    range_class: Type[BasePGRange[T]]

    def __init__(self, *ranges: Tuple[Union[
            BasePGRange[T],
            Tuple[Union[str, T, None], Optional[T], Optional[str]]]]) -> None:
        _ranges = (
            r if isinstance(r, self.range_class) else self.range_class(*r)
            for r in ranges)
        self._ranges = _normalize_ranges(r for r in _ranges if not r.is_empty)

    @property
    def lower(self) -> Union[T, None]:
        return self._ranges[0].lower if self._ranges else None

    @property
    def lower_inc(self) -> bool:
        return self._ranges[0].lower_inc if self._ranges else False

    @property
    def lower_inf(self) -> bool:
        return self._ranges[0].lower_inf if self._ranges else False

    @property
    def upper(self) -> Union[T, None]:
        return self._ranges[-1].upper if self._ranges else None

    @property
    def upper_inc(self) -> bool:
        return self._ranges[-1].upper_inc if self._ranges else False

    @property
    def upper_inf(self) -> bool:
        return self._ranges[-1].upper_inf if self._ranges else False

    @property
    def is_empty(self) -> bool:
        return not self._ranges

    def __contains__(
            self, item: Union['BaseMultiRange[T]', BasePGRange[T], T]) -> bool:
        if isinstance(item, BaseMultiRange):
            return all(r in self for r in item._ranges)
        return any(item in r for r in self._ranges)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self._ranges == other._ranges

    def __str__(self) -> str:
        return f"{{{','.join(str(r) for r in self._ranges)}}}"

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}"
            f"({', '.join(repr(r) for r in self._ranges)})")


class RangeFlags(Flag):
    RANGE_EMPTY = 0x01
    RANGE_LB_INC = 0x02
    RANGE_UB_INC = 0x04
    RANGE_LB_INF = 0x08
    RANGE_UB_INF = 0x10
    RANGE_LB_NULL = 0x20  # lower bound is null (NOT USED)
    RANGE_UB_NULL = 0x40  # upper bound is null (NOT USED)
    RANGE_CONTAIN_EMPTY = 0x80  # Something internal to postgres and GiST


class BinRangeResultConverter(Generic[T]):

    def __init__(
            self, range_class: Type[BasePGRange[T]], conv: ResConverter[Any]):
        self.range_class = range_class
        self.conv = conv

    def __call__(
            self,
            prot: 'pagio.base_protocol._AbstractPGProtocol',
            buf: memoryview,
    ) -> BasePGRange[T]:
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
                lower_len = int_from_bytes(buf[pos:pos + 4])
                pos += 4
                lower = self.conv(prot, buf[pos:pos + lower_len])
                pos += lower_len
            if flags & RangeFlags.RANGE_UB_INF:
                upper = None
            else:
                upper_len = int_from_bytes(buf[pos:pos + 4])
                pos += 4
                upper = self.conv(prot, buf[pos:pos + upper_len])
                pos += upper_len
        if pos != len(buf):
            raise ValueError("Invalid binary range value")
        return self.range_class(lower, upper, bounds)


class BinMultiRangeResultConverter(Generic[T]):

    def __init__(
            self,
            multirange_class: Type[BaseMultiRange[T]],
            conv: ResConverter[Any],
    ) -> None:
        self.multirange_class = multirange_class
        self.range_converter = BinRangeResultConverter(
            multirange_class.range_class, conv)

    def __call__(
            self,
            prot: 'pagio.base_protocol._AbstractPGProtocol',
            buf: memoryview,
    ) -> BaseMultiRange[T]:
        num_ranges = int.from_bytes(buf[:4], "big")
        ranges = []
        pos = 4
        for _ in range(num_ranges):
            range_len = int.from_bytes(buf[pos:pos + 4], "big")
            pos += 4
            ranges.append(self.range_converter(prot, buf[pos:pos + range_len]))
            pos += range_len
        if pos != len(buf):
            raise ValueError("Invalid binary multirange value.")
        return self.multirange_class(*ranges)


range_end = (ord(']'), right_parens)


class TxtRangeResultConverter(Generic[T]):

    def __init__(
            self, range_class: Type[BasePGRange[T]], conv: ResConverter[Any]):
        self.range_class = range_class
        self.conv = conv

    def parse(
            self,
            prot: 'pagio.base_protocol._AbstractPGProtocol',
            buf: memoryview,
    ) -> Tuple[BasePGRange[T], int]:
        if buf[:5] == b'empty':
            lower = None
            upper = None
            bounds = None
            pos = 5
        else:
            pos = 1
            if buf[pos] == quote:
                lower, new_pos = parse_quoted(buf[pos:], prot, self.conv)
                pos += new_pos
            elif buf[pos] == comma:
                lower = None
            else:
                lower, new_pos = parse_unquoted(
                    buf[pos:], (comma,), prot, self.conv)
                pos += new_pos
            if buf[pos] != comma:
                raise ValueError("Invalid range value.")
            pos += 1
            if buf[pos] == quote:
                upper, new_pos = parse_quoted(buf[pos:], prot, self.conv)
                pos += new_pos
            elif buf[pos] in range_end:
                upper = None
            else:
                upper, new_pos = parse_unquoted(
                    buf[pos:], range_end, prot, self.conv)
                pos += new_pos

            bounds = chr(buf[0]) + chr(buf[pos])
            pos += 1
        return self.range_class(lower, upper, bounds), pos

    def __call__(
            self,
            prot: 'pagio.base_protocol._AbstractPGProtocol',
            buf: memoryview,
    ) -> BasePGRange[T]:
        value, pos = self.parse(prot, buf)
        if pos != len(buf):
            raise ValueError("Invalid textual range value.")
        return value


class TxtMultiRangeResultConverter(Generic[T]):

    def __init__(
            self,
            multirange_class: Type[BaseMultiRange[T]],
            conv: ResConverter[Any],
    ) -> None:
        self.multirange_class = multirange_class
        self.range_converter = TxtRangeResultConverter(
            multirange_class.range_class, conv)

    def __call__(
            self,
            prot: 'pagio.base_protocol._AbstractPGProtocol',
            buf: memoryview,
    ) -> BaseMultiRange[T]:
        if buf[0] != ord('{'):
            raise ValueError("Invalid multirange value.")
        pos = 1
        ranges = []
        if buf[pos] == ord('}'):
            pos += 1
        else:
            while True:
                new_range, chars_read = self.range_converter.parse(
                    prot, buf[pos:])
                ranges.append(new_range)
                pos += chars_read
                curr_char = buf[pos]
                pos += 1
                if curr_char == ord('}'):
                    break
                if curr_char != comma:
                    raise ValueError("Invalid multirange value.")
        if pos != len(buf):
            raise ValueError("Invalid textual multirange value.")
        return self.multirange_class(*ranges)
