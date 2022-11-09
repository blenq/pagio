""" Text and bytea conversions """

from binascii import a2b_hex
from codecs import decode
from typing import Iterator, Generator
from uuid import UUID


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


def txt_uuid_to_python(buf: memoryview) -> UUID:
    """ Converts PG textual value to Python UUID """
    return UUID(decode(buf))


def bin_uuid_to_python(buf: memoryview) -> UUID:
    """ Converts PG binary value to Python UUID """
    return UUID(bytes=bytes(buf))
