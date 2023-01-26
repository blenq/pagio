from codecs import decode
from typing import Callable, TypeVar

import pagio

RCT = TypeVar('RCT', covariant=True)
ResConverter = Callable[
    ['pagio.base_protocol._AbstractPGProtocol', memoryview], RCT]


comma = ord(',')
right_parens = ord(')')


def _simple_conv(
        func: Callable[[memoryview], RCT]
) -> ResConverter[RCT]:

    def simple_conv(
            prot: 'pagio.base_protocol._AbstractPGProtocol',
            buf: memoryview) -> RCT:
        return func(buf)

    return simple_conv


simple_decode: ResConverter[str] = _simple_conv(decode)
simple_bytes = _simple_conv(bytes)
simple_int = _simple_conv(int)
