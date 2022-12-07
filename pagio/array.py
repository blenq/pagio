from struct import unpack_from
from typing import Any, List

from .common import ResConverter, ProtocolError

start_array = ord('{')
end_array = ord('}')
quote = ord('"')
backslash = ord('\\')


class ArrayConverter:

    def __init__(self, delimiter: str, converter: ResConverter) -> None:
        self._delim = ord(delimiter)
        self._converter = converter

    def _parse_quoted(self, buf: memoryview) -> Any:
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
                return self._converter(memoryview(bytes(chars))), pos + 1
            else:
                chars.append(char)
            pos += 1
        raise ProtocolError("Invalid array value.")

    def _parse_unquoted(self, buf: memoryview) -> Any:
        pos = 0
        buf_len = len(buf)
        while pos < buf_len:
            char = buf[pos]
            if char == self._delim or char == end_array:
                val_buf = buf[:pos]
                if val_buf == b'NULL':
                    val = None
                else:
                    val = self._converter(val_buf)
                return val, pos
            pos += 1
        raise ProtocolError("Invalid array value.")

    def _parse_array(self, buf: memoryview) -> List[Any]:
        i = 1
        buf_len = len(buf)
        vals = []
        while i < buf_len:
            char = buf[i]
            if char == start_array:
                item, pos = self._parse_array(buf[i:])
                vals.append(item)
                i += pos
            elif char == quote:
                item, pos = self._parse_quoted(buf[i:])
                vals.append(item)
                i += pos
            elif char != end_array:
                item, pos = self._parse_unquoted(buf[i:])
                vals.append(item)
                i += pos

            char = buf[i]
            if char == end_array:
                return vals, i + 1
            if char == self._delim:
                i += 1
            else:
                raise ProtocolError("Invalid array value")

        raise ProtocolError("Invalid array value")

    def to_python(self, buf: memoryview) -> List[Any]:
        i = 0
        buf_len = len(buf)
        while i < buf_len:
            # skip optional array dims
            if buf[i] == start_array:
                item, pos = self._parse_array(buf[i:])
                if i + pos != buf_len:
                    raise ProtocolError("Invalid array value")
                return item
            i += 1
        raise ProtocolError("Invalid array value")


class BinArrayConverter:
    def __init__(self, type_oid: int, converter: ResConverter) -> None:
        self._oid = type_oid
        self._converter = converter

    def _get_values(self, buf: memoryview, array_dims: List[int]):
        if array_dims:
            # get an array of (nested) values
            dim = array_dims[0]
            i = 0
            vals = []
            for _ in range(dim):
                val, pos = self._get_values(buf[i:], array_dims[1:])
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
        return self._converter(val_buf), full_length

    def to_python(self, buf: memoryview):
        dims, flags, elem_type = unpack_from("!IiI", buf, 0)

        if elem_type != self._oid:
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
        vals, vals_pos = self._get_values(buf[pos:], array_dims)
        pos += vals_pos
        if pos != len(buf):
            raise ProtocolError("Invalid array value")
        return vals
