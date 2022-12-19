from codecs import decode
from typing import Dict

from .array import quote, parse_quoted, parse_unquoted
from .numeric import bin_int_to_python


comma = ord(',')
space = ord(' ')
equals = ord('=')
gt = ord('>')
hstore_delims = (comma, 0)


def txt_hstore_to_python(conn, buf: memoryview) -> Dict[str, str]:
    pos = 0
    buf_len = len(buf)
    hstore_val = {}
    while pos < buf_len:
        if buf[pos] != quote:
            raise ValueError("Invalid hstore")
        key, parse_pos = parse_quoted(buf[pos:], conn)
        pos += parse_pos
        if buf[pos] != equals:
            raise ValueError("Invalid hstore value.")
        pos += 1
        if buf[pos] != gt:
            raise ValueError("Invalid hstore value.")
        pos += 1
        if buf[pos] == quote:
            value, parse_pos = parse_quoted(buf[pos:], conn)
        else:
            value, parse_pos = parse_unquoted(buf[pos:], hstore_delims, conn)
        pos += parse_pos
        hstore_val[key] = value
        if pos < buf_len:
            if buf[pos] != comma:
                raise ValueError("Invalid hstore value.")
            pos += 1
            while buf[pos] == space:
                pos += 1
    return hstore_val


def bin_hstore_to_python(conn, buf: memoryview) -> Dict[str, str]:
    buf_len = len(buf)
    hstore_val = {}
    if buf_len < 4:
        raise ValueError("Invalid hstore value.")
    num_vals = bin_int_to_python(conn, buf[:4])
    pos = 4
    while pos < buf_len:
        item_len = bin_int_to_python(conn, buf[pos:pos + 4])
        pos += 4
        if item_len < 0:
            raise ValueError("Invalid hstore value")
        key = decode(buf[pos:pos + item_len])
        pos += item_len
        item_len = bin_int_to_python(conn, buf[pos:pos + 4])
        pos += 4
        if item_len == -1:
            val = None
        elif item_len < 0:
            raise ValueError("Invalid hstore value")
        else:
            val = decode(buf[pos:pos + item_len])
            pos += item_len
        hstore_val[key] = val
    if num_vals != len(hstore_val):
        raise ValueError("Invalid hstore value.")
    return hstore_val
