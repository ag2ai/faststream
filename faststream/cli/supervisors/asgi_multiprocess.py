import inspect
from pathlib import Path
from typing import Dict, Optional, Tuple

from faststream.asgi.app import cast_uvicorn_params


class ASGIMultiprocess:
    def __init__(
        self,
        target: str,
        args: Tuple[str, Dict[str, str], bool, Optional[Path], int],
        workers: int,
    ) -> None:
        _, uvicorn_kwargs, is_factory, _, log_level = args
        self._target = target
        self._uvicorn_kwargs = cast_uvicorn_params(uvicorn_kwargs or {})
        self._workers = workers
        self._is_factory = is_factory
        self._log_level = log_level

    def run(self) -> None:
        try:
            import uvicorn
        except ImportError as e:
            raise ImportError(
                "You need uvicorn to run FastStream ASGI App via CLI. pip install uvicorn"
            ) from e

        uvicorn_params = set(inspect.signature(uvicorn.run).parameters.keys())

        uvicorn.run(
            self._target,
            factory=self._is_factory,
            workers=self._workers,
            log_level=self._log_level,
            **{
                key: v
                for key, v in self._uvicorn_kwargs.items()
                if key in uvicorn_params
            },
        )
