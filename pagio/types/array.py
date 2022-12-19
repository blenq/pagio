from codecs import decode
import re
from struct import unpack_from
from typing import Any, Callable, List, Collection, Tuple, TypeVar

from ..common import ResConverter, ProtocolError

start_array = ord('{')
end_array = ord('}')
quote = ord('"')
backslash = ord('\\')


def simple_decode(conn, buf: memoryview) -> str:
    return decode(buf)


T = TypeVar('T')


def parse_quoted(buf: memoryview, conn, converter=simple_decode) -> Any:
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
        conn,
        converter: Callable[[Any, memoryview], T] = simple_decode,
) -> Tuple[T, int]:
    pos = 0
    buf_len = len(buf)
    while pos < buf_len:
        char = buf[pos]
        if char in delims:
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


class ArrayConverter:

    def __init__(self, delimiter: str, converter: ResConverter) -> None:
        self._delims = [ord(c) for c in delimiter + "}"]
        self._converter = converter

    def _parse_array(self, conn, buf: memoryview) -> List[Any]:
        i = 1
        buf_len = len(buf)
        vals = []
        while i < buf_len:
            char = buf[i]
            if char == start_array:
                item, pos = self._parse_array(conn, buf[i:])
                vals.append(item)
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

    def __call__(self, conn, buf: memoryview) -> List[Any]:
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


class BinArrayConverter:
    def __init__(self, elem_oid: int, converter: ResConverter) -> None:
        self._elem_oid = elem_oid
        self._converter = converter

    def _get_values(self, conn, buf: memoryview, array_dims: List[int]):
        if array_dims:
            # get an array of (nested) values
            dim = array_dims[0]
            i = 0
            vals = []
            for _ in range(dim):
                val, pos = self._get_values(conn, buf[i:], array_dims[1:])
                vals.append(val)
                i += pos
            return vals, i

        # get a single value, either NULL or an actual value prefixed by a
        # length
        [item_len] = unpack_from("!i", buf, 0)
        if item_len == -1:
            return None, 4
        full_length = 4 + item_len
        val_buf = buf[4:full_length]
        if item_len > len(val_buf):
            raise ProtocolError("Invalid array value.")
        return self._converter(conn, val_buf), full_length

    def __call__(self, conn, buf: memoryview):
        dims, flags, elem_type = unpack_from("!IiI", buf, 0)

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
            unpack_from("!ii", buf, pos + i * 8)[0] for i in range(dims)]
        pos += 8 * dims
        vals, vals_pos = self._get_values(conn, buf[pos:], array_dims)
        pos += vals_pos
        if pos != len(buf):
            raise ProtocolError("Invalid array value")
        return vals


class PGArray:
    oid: int = 0
    delimiter: str = ","
    ws_pattern = re.compile("[\\s{}\"\']")

    def __init__(self, vals: List[Any]):
        self._vals = vals

    def _val_to_str(self, val):
        return str(val)

    def _get_vals(self):
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

    def __str__(self):
        return f"{{{self.delimiter.join(self._get_vals())}}}"

    def __repr__(self):
        return f"{self.__class__.__name__}({repr(self._vals)})"
