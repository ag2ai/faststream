import inspect
from typing import TYPE_CHECKING, Any

from faststream._internal._compat import HAS_UVICORN, uvicorn
from faststream.asgi.app import cast_uvicorn_params
from faststream.exceptions import INSTALL_UVICORN

if TYPE_CHECKING:
    from faststream._internal.basic_types import SettingField
    from faststream._internal.cli.dto import RunArgs

if HAS_UVICORN:
    from uvicorn.supervisors.multiprocess import Multiprocess, Process

    class UvicornExtraConfig(uvicorn.Config):  # type: ignore[misc]
        def __init__(
            self,
            run_extra_options: dict[str, "SettingField"],
            *args: Any,
            **kwargs: Any,
        ) -> None:
            super().__init__(*args, **kwargs)
            self._run_extra_options = run_extra_options

        def load(self) -> None:
            super().load()
            self.loaded_app.app._run_extra_options = self._run_extra_options

    class UvicornMultiprocess(Multiprocess):
        config: UvicornExtraConfig

        def init_processes(self) -> None:
            for i in range(self.processes_num):
                self.config._run_extra_options["worker_id"] = i
                process = Process(self.config, self.target, self.sockets)
                process.start()
                self.processes.append(process)


class ASGIMultiprocess:
    def __init__(
        self,
        target: str,
        args: "RunArgs",
        workers: int,
    ) -> None:
        self._target = target
        self._run_extra_options = cast_uvicorn_params(args.extra_options)
        self._workers = workers
        self._is_factory = args.is_factory
        self._log_level = args.log_level

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
