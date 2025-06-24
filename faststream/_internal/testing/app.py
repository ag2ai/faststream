from contextlib import ExitStack
from functools import partial
from typing import TYPE_CHECKING, Optional

from anyio.from_thread import start_blocking_portal

if TYPE_CHECKING:
    from types import TracebackType

    from faststream._internal.application import Application
    from faststream._internal.basic_types import SettingField


class TestApp:
    """A class to represent a test application."""

    __test__ = False

    app: "Application"
    _extra_options: dict[str, "SettingField"]

    def __init__(
        self,
        app: "Application",
        run_extra_options: dict[str, "SettingField"] | None = None,
    ) -> None:
        self.app = app
        self._extra_options = run_extra_options or {}

    def __enter__(self) -> "Application":
        with ExitStack() as stack:
            portal = stack.enter_context(start_blocking_portal())

            lifespan_context = self.app.lifespan_context(**self._extra_options)
            stack.enter_context(portal.wrap_async_context_manager(lifespan_context))
            portal.call(partial(self.app.start, **self._extra_options))

            @stack.callback
            def wait_shutdown() -> None:
                portal.call(self.app.stop)

            self.exit_stack = stack.pop_all()

        return self.app

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        self.exit_stack.close()

    async def __aenter__(self) -> "Application":
        self.lifespan_scope = self.app.lifespan_context(**self._extra_options)
        await self.lifespan_scope.__aenter__()
        await self.app.start(**self._extra_options)
        return self.app

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        """Exit the asynchronous context manager."""
        await self.app.stop()
        await self.lifespan_scope.__aexit__(exc_type, exc_val, exc_tb)
