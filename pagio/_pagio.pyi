from typing import Tuple, Any, List

from .base_protocol import Format


class CBasePGProtocol:

    def get_buffer(self, sizehint: int) -> memoryview:
        ...

    def buffer_updated(self, nbytes: int) -> None:
        ...

    def execute_message(
        self,
        sql: str,
        parameters: Tuple[Any, ...],
        result_format: Format,
    ) -> List[bytes]:
        ...