from ssl import SSLSocket, SSLObject
from typing import Union, List, Tuple, Optional

from . import core, utils

class ScramClient:

    password: str
    mechanism_name: str
    c_nonce: str
    channel_binding: Optional[Tuple[str, bytes]]

    def __init__(
            self,
            mechanisms: Union[List[str], Tuple[str]],
            username: str,
            password: str,
            channel_binding: Optional[Tuple[str, bytes]] = None,
            c_nonce: Optional[str] = None,
    ):
        ...

    def get_client_first(self) -> str:
        ...

    def set_server_first(self, message: str) -> None:
        ...

    def get_client_final(self) -> str:
        ...

    def set_server_final(self, message: str) -> None:
        ...


class ScramException(Exception):
    server_error: Optional[str]


def make_channel_binding(
        name: str, ssl_socket: Union[SSLSocket, SSLObject],
) -> Tuple[str, bytes]:
    ...
