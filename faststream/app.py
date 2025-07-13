import logging
from typing import (
    TYPE_CHECKING,
    AsyncIterator,
    Dict,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
)

from typing_extensions import ParamSpec

from faststream._internal.application import Application
from faststream.asgi.app import AsgiFastStream
from faststream.cli.supervisors.utils import set_exit
from faststream.exceptions import ValidationError

P_HookParams = ParamSpec("P_HookParams")
T_HookReturn = TypeVar("T_HookReturn")


if TYPE_CHECKING:
    from faststream.asgi.types import ASGIApp
    from faststream.types import SettingField


class FastStream(Application):
    """A class representing a FastStream application."""

    async def run(
        self,
        log_level: int = logging.INFO,
        run_extra_options: Optional[Dict[str, "SettingField"]] = None,
        sleep_time: float = 0.1,
    ) -> None:
        """Run FastStream Application."""
        set_exit(lambda *_: self.exit(), sync=False)

        async with catch_startup_validation_error(), self.lifespan_context(
            **(run_extra_options or {})
        ):
            await self._startup(log_level, run_extra_options)
            await self._main_loop(sleep_time)
            await self._shutdown(log_level)

    def as_asgi(
        self,
        asgi_routes: Sequence[Tuple[str, "ASGIApp"]] = (),
        asyncapi_path: Optional[str] = None,
    ) -> AsgiFastStream:
        return AsgiFastStream.from_app(self, asgi_routes, asyncapi_path)


try:
    from contextlib import asynccontextmanager

    from pydantic import ValidationError as PValidation

    @asynccontextmanager
    async def catch_startup_validation_error() -> AsyncIterator[None]:
        try:
            yield
        except PValidation as e:
            fields = [str(x["loc"][0]) for x in e.errors()]
            raise ValidationError(fields=fields) from e

except ImportError:
    from faststream.utils.functions import fake_context

    catch_startup_validation_error = fake_context
