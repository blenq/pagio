""" Text and bytea conversions """
import json
from binascii import a2b_hex
from codecs import decode
from json import loads, dumps, JSONEncoder
from typing import Iterator, Generator, Any, Tuple, Optional, Type
from uuid import UUID

import pagio

from .array import PGArray
from ..common import Format, ProtocolError
from ..const import (
    UUIDOID, BYTEAOID, JSONBOID, TEXTOID, TEXTARRAYOID, UUIDARRAYOID,
    JSONBARRAYOID, UNKNOWNOID, REGCONFIGOID)

# ======== bytea ============================================================ #


def txt_bytea_to_python(
        prot: 'pagio.base_protocol._AbstractPGProtocol',
        buf: memoryview,
) -> bytes:
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


def bytes_to_pg(val: bytes) -> Tuple[int, str, bytes, int, Format]:
    """ Converts Python bytes valye to PG bytea value """
    val_len = len(val)
    return BYTEAOID, f"{val_len}s", val, val_len, Format.BINARY

# ======== uuid ============================================================= #


def txt_uuid_to_python(
        prot: 'pagio.base_protocol._AbstractPGProtocol',
        buf: memoryview,
) -> UUID:
    """ Converts PG textual value to Python UUID """
    return UUID(decode(buf))


def bin_uuid_to_python(
        prot: 'pagio.base_protocol._AbstractPGProtocol',
        buf: memoryview,
) -> UUID:
    """ Converts PG binary value to Python UUID """
    return UUID(bytes=bytes(buf))


def uuid_to_pg(val: UUID) -> Tuple[int, str, bytes, int, Format]:
    """ Converts Python UUID value to PG uuid parameter """
    return UUIDOID, "16s", val.bytes, 16, Format.BINARY


class PGUUIDArray(PGArray):
    oid = UUIDARRAYOID


# ======== text ============================================================= #


def str_to_pg(val: str, oid: Optional[int] = None) -> Tuple[int, str, bytes, int, Format]:
    """ Convert a Python string to a PG text parameter """
    bytes_val = val.encode()
    val_len = len(bytes_val)
    if oid is None:
        oid = 0
    return oid, f"{val_len}s", bytes_val, val_len, Format.TEXT


def default_to_pg(val: Any) -> Tuple[int, str, bytes, int, Format]:
    """ Convert a Python object to a PG text parameter """
    oid = getattr(val, "oid", 0)
    return str_to_pg(str(val), oid)

# ======== jsonb ============================================================ #


def txt_json_to_python(
        prot: 'pagio.base_protocol._AbstractPGProtocol',
        buf: memoryview,
) -> Any:
    """ Converts textual PG json to Python """
    return loads(decode(buf))


def bin_jsonb_to_python(
        prot: 'pagio.base_protocol._AbstractPGProtocol',
        buf: memoryview,
) -> Any:
    """ Converts binary PG jsonb to Python """
    if buf[0] != 1:
        raise ProtocolError("Invalid jsonb version")
    return loads(decode(buf[1:]))


class PGJson:  # pylint: disable=too-few-public-methods
    """ Class to facilitate JSON PG parameter """

    oid = JSONBOID

    def __init__(
            self,
            val: Any,
            *,
            cls: Optional[Type[JSONEncoder]] = None,
    ) -> None:
        self._val = val
        self._encoder = cls

    def __str__(self) -> str:
        return dumps(self._val, cls=self._encoder)

    def __repr__(self) -> str:
        return f"PGJson({repr(self._val)})"


class PGText(str):
    oid = TEXTOID

    def __repr__(self) -> str:
        return f"PGText({super().__repr__()})"


class PGRegConfig(str):
    oid = REGCONFIGOID

    def __repr__(self) -> str:
        return f"PGRegConfig({super().__repr__()})"


class PGTextArray(PGArray):
    oid = TEXTARRAYOID


class PGJsonArray(PGArray):
    oid = JSONBARRAYOID
