import inspect
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

from faststream._compat import HAS_UVICORN, UvicornMultiprocess, uvicorn
from faststream.asgi.app import cast_uvicorn_params
from faststream.exceptions import INSTALL_UVICORN

if TYPE_CHECKING:
    from faststream.types import SettingField


if HAS_UVICORN:

    class UvicornExtraConfig(uvicorn.Config):  # type: ignore[misc]
        def __init__(
            self,
            run_extra_options: Dict[str, "SettingField"],
            *args: Any,
            **kwargs: Any,
        ) -> None:
            super().__init__(*args, **kwargs)
            self._run_extra_options = run_extra_options

        def load(self) -> None:
            super().load()
            self.loaded_app.app._run_extra_options = self._run_extra_options


class ASGIMultiprocess:
    def __init__(
        self,
        target: str,
        args: Tuple[str, Dict[str, str], bool, Optional[Path], int],
        workers: int,
    ) -> None:
        _, run_extra_options, is_factory, _, log_level = args
        self._target = target
        self._run_extra_options = cast_uvicorn_params(run_extra_options or {})
        self._workers = workers
        self._is_factory = is_factory
        self._log_level = log_level

    def run(self) -> None:
        if not HAS_UVICORN:
            raise ImportError(INSTALL_UVICORN)

        config = UvicornExtraConfig(
            app=self._target,
            factory=self._is_factory,
            log_level=self._log_level,
            workers=self._workers,
            **{
                key: v
                for key, v in self._run_extra_options.items()
                if key in set(inspect.signature(uvicorn.Config).parameters.keys())
            },
            run_extra_options=self._run_extra_options,
        )
        server = uvicorn.Server(config)
        sock = config.bind_socket()
        UvicornMultiprocess(config, target=server.run, sockets=[sock]).run()
