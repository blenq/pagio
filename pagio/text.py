""" Text and bytea conversions """

from binascii import a2b_hex
from codecs import decode
from json import loads
from typing import Iterator, Generator, Any, Tuple
from uuid import UUID

from .common import Format, ProtocolError
from .const import UUIDOID

# ======== bytea ============================================================ #


def txt_bytea_to_python(buf: memoryview) -> bytes:
    """ Converts a PG textual bytea value to Python bytes object """

    if buf[:2] == b'\\x':
        # hexadecimal encoding
        return a2b_hex(buf[2:])

    # escape encoding
    def next_or_fail(iterator: Iterator[int]) -> int:
        try:
            ret = next(iterator)
        except StopIteration as exc:
            raise ValueError("Invalid bytea value") from exc
        return ret

    def get_bytes() -> Generator[int, None, None]:
        backslash = ord(b'\\')
        biter = iter(buf)
        for byte_val in biter:
            if byte_val != backslash:
                # regular byte
                yield byte_val
                continue

            byte_val = next_or_fail(biter)
            if byte_val == backslash:
                # backslash
                yield byte_val
                continue

            # octal value
            byte2 = next_or_fail(biter) - 48
            byte3 = next_or_fail(biter) - 48
            yield (byte_val - 48) * 64 + byte2 * 8 + byte3

    return bytes(get_bytes())

# ======== uuid ============================================================= #


def txt_uuid_to_python(buf: memoryview) -> UUID:
    """ Converts PG textual value to Python UUID """
    return UUID(decode(buf))


def bin_uuid_to_python(buf: memoryview) -> UUID:
    """ Converts PG binary value to Python UUID """
    return UUID(bytes=bytes(buf))


def uuid_to_pg(val: UUID) -> Tuple[int, str, bytes, int, Format]:
    return UUIDOID, "16s", val.bytes, 16, Format.BINARY

# ======== text ============================================================= #


def str_to_pg(val: str) -> Tuple[int, str, bytes, int, Format]:
    """ Convert a Python string to a PG text parameter """
    bytes_val = val.encode()
    val_len = len(bytes_val)
    return 0, f"{val_len}s", bytes_val, val_len, Format.TEXT


def default_to_pg(val: Any) -> Tuple[int, str, bytes, int, Format]:
    """ Convert a Python object to a PG text parameter """
    return str_to_pg(str(val))

# ======== jsonb ============================================================ #


def txt_json_to_python(buf: memoryview) -> Any:
    return loads(decode(buf))


def bin_jsonb_to_python(buf: memoryview) -> Any:
    if buf[0] != 1:
        raise ProtocolError("Invalid jsonb version")
    return loads(decode(buf[1:]))
