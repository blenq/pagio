from typing import Callable, Any


def _make_salted_password(
        hf: Callable[[], Any],
        password: str,
        salt: bytes,
        iterations: int,
    ) -> str:
    ...


def saslprep(source: str) -> str:
    ...
