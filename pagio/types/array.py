import re
from typing import (
    Any, Generator, List, Collection, Tuple, TypeVar, Optional, Generic)

import pagio

from ..common import ProtocolError, int_from_bytes, uint_from_bytes
from .conv_utils import simple_decode, ResConverter

start_array = ord('{')
end_array = ord('}')
quote = ord('"')
backslash = ord('\\')


T = TypeVar('T')


def parse_quoted(
        buf: memoryview,
        conn: 'pagio.base_protocol._AbstractPGProtocol',
        converter: ResConverter[T] = simple_decode,  # type: ignore
) -> Tuple[T, int]:
    escaped = False
    pos = 1
    buf_len = len(buf)
    chars = []
    while pos < buf_len:
        char = buf[pos]
        if escaped:
            chars.append(char)
            escaped = False
        elif char == backslash:
            escaped = True
        elif char == quote:
            if pos + 1 < buf_len and buf[pos + 1] == quote:
                # double escaped " for hstore
                escaped = True
            else:
                return converter(conn, memoryview(bytes(chars))), pos + 1
        else:
            chars.append(char)
        pos += 1
    raise ProtocolError("Invalid array value.")


def parse_unquoted(
        buf: memoryview,
        delims: Collection[int],
        conn: 'pagio.base_protocol._AbstractPGProtocol',
        converter: ResConverter[T] = simple_decode,  # type: ignore
) -> Tuple[Optional[T], int]:
    pos = 0
    # for pos, c in enumerate(buf):
    #     if c in delims:
    #         break
    buf_len = len(buf)
    while pos < buf_len:
        if buf[pos] in delims:
            break
        pos += 1
    else:
        if 0 not in delims:
            raise ProtocolError("Invalid array value.")

    val_buf = buf[:pos]
    if val_buf == b'NULL':
        val = None
    else:
        val = converter(conn, val_buf)
    return val, pos


class ArrayConverter(Generic[T]):

    def __init__(self, delimiter: str, converter: ResConverter[T]) -> None:
        self._delims = [ord(c) for c in delimiter + "}"]
        self._converter = converter

    def _parse_array(
            self,
            conn: 'pagio.base_protocol._AbstractPGProtocol',
            buf: memoryview,
    ) -> Tuple[List[Any], int]:
        i = 1
        buf_len = len(buf)
        vals: List[Any] = []
        item: Optional[T]
        while i < buf_len:
            char = buf[i]
            if char == start_array:
                list_item, pos = self._parse_array(conn, buf[i:])
                vals.append(list_item)
                i += pos
            elif char == quote:
                item, pos = parse_quoted(
                    buf[i:], conn, self._converter)
                vals.append(item)
                i += pos
            elif char != end_array:
                item, pos = parse_unquoted(
                    buf[i:], self._delims, conn, self._converter)
                vals.append(item)
                i += pos

            char = buf[i]
            if char == end_array:
                return vals, i + 1
            if char in self._delims:
                i += 1
            else:
                raise ProtocolError("Invalid array value")

        raise ProtocolError("Invalid array value")

    def __call__(
            self,
            conn: 'pagio.base_protocol._AbstractPGProtocol',
            buf: memoryview,
    ) -> List[Any]:
        i = 0
        buf_len = len(buf)
        while i < buf_len:
            # skip optional array dims
            if buf[i] == start_array:
                item, pos = self._parse_array(conn, buf[i:])
                if i + pos != buf_len:
                    raise ProtocolError("Invalid array value")
                return item
            i += 1
        raise ProtocolError("Invalid array value")


class BinArrayConverter(Generic[T]):
    def __init__(self, elem_oid: int, converter: ResConverter[T]) -> None:
        self._elem_oid = elem_oid
        self._converter = converter

    def _get_values(
            self,
            prot: 'pagio.base_protocol._AbstractPGProtocol',
            buf: memoryview,
            array_dims: List[int],
    ) -> Tuple[Any, int]:
        if array_dims:
            # get an array of (nested) values
            dim = array_dims[0]
            i = 0
            vals = []
            for _ in range(dim):
                val, pos = self._get_values(prot, buf[i:], array_dims[1:])
                vals.append(val)
                i += pos
            return vals, i

        # get a single value, either NULL or an actual value prefixed by a
        # length
        item_len = int_from_bytes(buf[:4])
        if item_len == -1:
            return None, 4
        full_length = 4 + item_len
        val_buf = buf[4:full_length]
        if item_len > len(val_buf):
            raise ProtocolError("Invalid array value.")
        return self._converter(prot, val_buf), full_length

    def __call__(
            self,
            prot: 'pagio.base_protocol._AbstractPGProtocol',
            buf: memoryview,
    ) -> Any:
        dims = uint_from_bytes(buf[:4])
        flags = int_from_bytes(buf[4:8])
        elem_type = uint_from_bytes(buf[8:12])

        if elem_type != self._elem_oid:
            raise ProtocolError("Unexpected element type")
        if dims > 6:
            raise ProtocolError("Number of dimensions exceeded")
        if flags & 1 != flags:
            raise ProtocolError("Invalid value for array flags")
        if dims == 0:
            return []
        pos = 12
        array_dims = [
            int_from_bytes(buf[pos + i * 8: pos + i * 8 + 4])
            for i in range(dims)]
        pos += 8 * dims
        vals, vals_pos = self._get_values(prot, buf[pos:], array_dims)
        pos += vals_pos
        if pos != len(buf):
            raise ProtocolError("Invalid array value")
        return vals


class PGArray:
    oid: int = 0
    delimiter: str = ","
    ws_pattern = re.compile("[\\s{}\"\']")

    def __init__(self, vals: List[Any]) -> None:
        self._vals = vals

    def _val_to_str(self, val: Any) -> str:
        return str(val)

    def _get_vals(self) -> Generator[str, None, None]:
        for val in self._vals:
            if val is None:
                yield "NULL"
                continue
            if isinstance(val, list):
                yield str(self.__class__(val))
                continue
            val = self._val_to_str(val)
            if self.ws_pattern.search(val) or self.delimiter in val:
                val = val.replace('\\', '\\\\').replace('"', '\\"')
                yield f'"{val}"'
            else:
                yield val

    def __str__(self) -> str:
        return f"{{{self.delimiter.join(self._get_vals())}}}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({repr(self._vals)})"
