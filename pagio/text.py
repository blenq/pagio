from binascii import a2b_hex
from codecs import decode
from uuid import UUID


def txt_bytea_to_python(buf: memoryview):
    if buf[:2] == b'\\x':
        return a2b_hex(buf[2:])
    # escape encoding

    def next_or_fail(iterator):
        try:
            ret = next(iterator)
        except StopIteration:
            raise ValueError("Invalid bytea value")
        return ret

    def get_bytes():
        backslash = ord(b'\\')
        biter = iter(buf)
        for b in biter:
            if b != backslash:
                # regular byte
                yield b
                continue

            b = next_or_fail(biter)
            if b == backslash:
                # backslash
                yield b
                continue

            # octal value
            b2 = next_or_fail(biter) - 48
            b3 = next_or_fail(biter) - 48
            yield (b - 48) * 64 + b2 * 8 + b3

    return bytes(get_bytes())


def txt_uuid_to_python(buf: memoryview):
    return UUID(decode(buf))


def bin_uuid_to_python(buf: memoryview):
    return UUID(bytes=bytes(buf))
