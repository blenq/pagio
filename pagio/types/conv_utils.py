from codecs import decode
from typing import Any, Callable, TypeVar


T = TypeVar('T')


def _simple_conv(
        func: Callable[[memoryview], T]) -> Callable[[Any, memoryview], T]:

    def simple_conv(conn, buf: memoryview) -> T:
        return func(buf)

    return simple_conv


simple_decode: Callable[[memoryview], str] = _simple_conv(decode)
simple_bytes = _simple_conv(bytes)
simple_int: Callable[[memoryview], int] = _simple_conv(int)
