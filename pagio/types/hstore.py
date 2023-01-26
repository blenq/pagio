from codecs import decode
from typing import Dict, Optional

import pagio

from .array import quote, parse_quoted, parse_unquoted
from .conv_utils import comma
from .numeric import bin_int_to_python


space = ord(' ')
equals = ord('=')
gt = ord('>')
hstore_delims = (comma, 0)


def txt_hstore_to_python(
        prot: 'pagio.base_protocol._AbstractPGProtocol',
        buf: memoryview,
) -> Dict[str, Optional[str]]:
    pos = 0
    buf_len = len(buf)
    hstore_val = {}
    while pos < buf_len:
        if buf[pos] != quote:
            raise ValueError("Invalid hstore")
        key: str
        key, parse_pos = parse_quoted(buf[pos:], prot)
        pos += parse_pos
        if buf[pos] != equals:
            raise ValueError("Invalid hstore value.")
        pos += 1
        if buf[pos] != gt:
            raise ValueError("Invalid hstore value.")
        pos += 1
        value: Optional[str]
        if buf[pos] == quote:
            value, parse_pos = parse_quoted(buf[pos:], prot)
        else:
            value, parse_pos = parse_unquoted(buf[pos:], hstore_delims, prot)
        pos += parse_pos
        hstore_val[key] = value
        if pos < buf_len:
            if buf[pos] != comma:
                raise ValueError("Invalid hstore value.")
            pos += 1
            while buf[pos] == space:
                pos += 1
    return hstore_val


def bin_hstore_to_python(
        prot: 'pagio.base_protocol._AbstractPGProtocol',
        buf: memoryview,
) -> Dict[str, Optional[str]]:
    buf_len = len(buf)
    hstore_val = {}
    if buf_len < 4:
        raise ValueError("Invalid hstore value.")
    num_vals = bin_int_to_python(prot, buf[:4])
    pos = 4
    while pos < buf_len:
        item_len = bin_int_to_python(prot, buf[pos:pos + 4])
        pos += 4
        if item_len < 0:
            raise ValueError("Invalid hstore value")
        key = decode(buf[pos:pos + item_len])
        pos += item_len
        item_len = bin_int_to_python(prot, buf[pos:pos + 4])
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
