import logging
from collections.abc import Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    TypeVar,
)

import anyio
from typing_extensions import ParamSpec

from faststream._internal._compat import ExceptionGroup
from faststream._internal.application import Application
from faststream._internal.basic_types import Lifespan, LoggerProto
from faststream._internal.broker.broker import BrokerUsecase
from faststream._internal.cli.supervisors.utils import set_exit
from faststream._internal.constants import EMPTY
from faststream._internal.log import logger
from faststream.asgi.app import AsgiFastStream

if TYPE_CHECKING:
    from fast_depends import Provider
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import (
        AnyCallable,
        Lifespan,
        LoggerProto,
        SettingField,
    )
    from faststream._internal.broker.broker import BrokerUsecase
    from faststream.asgi.types import ASGIApp

P_HookParams = ParamSpec("P_HookParams")
T_HookReturn = TypeVar("T_HookReturn")


class FastStream(Application):
    """A class representing a FastStream application."""

    def __init__(
        self,
        broker: Optional["BrokerUsecase[Any, Any]"] = None,
        /,
        # regular broker args
        logger: Optional["LoggerProto"] = logger,
        provider: Optional["Provider"] = None,
        serializer: Optional["SerializerProto"] = EMPTY,
        lifespan: Optional["Lifespan"] = None,
        on_startup: Sequence["AnyCallable"] = (),
        after_startup: Sequence["AnyCallable"] = (),
        on_shutdown: Sequence["AnyCallable"] = (),
        after_shutdown: Sequence["AnyCallable"] = (),
    ) -> None:
        super().__init__(
            broker,
            logger=logger,
            provider=provider,
            serializer=serializer,
            lifespan=lifespan,
            on_startup=on_startup,
            after_startup=after_startup,
            on_shutdown=on_shutdown,
            after_shutdown=after_shutdown,
        )
        self._should_exit = False

    async def run(
        self,
        log_level: int = logging.INFO,
        run_extra_options: Optional[dict[str, "SettingField"]] = None,
        sleep_time: float = 0.1,
    ) -> None:
        """Run FastStream Application."""
        set_exit(lambda *_: self.exit(), sync=False)

        async with self.lifespan_context(**(run_extra_options or {})):
            try:
                async with anyio.create_task_group() as tg:
                    tg.start_soon(self._startup, log_level, run_extra_options)

                    while not self._should_exit:  # noqa: ASYNC110 (requested by creator)
                        await anyio.sleep(sleep_time)

                    await self._shutdown(log_level)
                    tg.cancel_scope.cancel()
            except ExceptionGroup as e:
                for ex in e.exceptions:
                    raise ex from None

    def exit(self) -> None:
        """Stop application manually."""
        self._should_exit = True

    def as_asgi(
        self,
        asgi_routes: Sequence[tuple[str, "ASGIApp"]] = (),
    ) -> AsgiFastStream:
        return AsgiFastStream.from_app(self, asgi_routes)
