from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any, Protocol


class GenerateTemplateFactory(Protocol):
    def __call__(
        self,
        code: str,
        filename: str = "temp_app.py",
    ) -> AbstractContextManager[Path]: ...


class FastStreamCLIFactory(Protocol):
    def __call__(
        self,
        *cmd: str,
        wait_time: float = 2.0,
        extra_env: dict[str, str] | None = None,
    ) -> AbstractContextManager[Any]: ...
