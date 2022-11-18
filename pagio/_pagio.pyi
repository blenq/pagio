from typing import Tuple, Any, List, Optional

from .common import Format, CopyFile


class CBasePGProtocol:

    _cache_size: int
    _prepare_threshold: int

    def get_buffer(self, sizehint: int) -> memoryview:
        ...

    def buffer_updated(self, nbytes: int) -> None:
        ...

    def execute_message(
        self,
        sql: str,
        parameters: Tuple[Any, ...],
        result_format: Format,
        raw_result: bool,
        file_obj: Optional[CopyFile],
    ) -> List[bytes]:
        ...

    def _setup_ssl_request(self) -> None:
        ...
