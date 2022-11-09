from typing import Any, Callable


def hi(hf: Callable[[], Any], password: bytes, salt: bytes, iterations: int
       ) -> str:
    ...
