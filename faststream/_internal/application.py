import logging
from abc import abstractmethod
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
    TypeVar,
)

from fast_depends import Provider
from typing_extensions import ParamSpec

from faststream._internal.constants import EMPTY
from faststream._internal.context import ContextRepo
from faststream._internal.log import logger
from faststream._internal.state import DIState
from faststream._internal.state.application import (
    ApplicationState,
    BasicApplicationState,
    RunningApplicationState,
)
from faststream._internal.state.broker import OuterBrokerState
from faststream._internal.utils import apply_types
from faststream._internal.utils.functions import (
    drop_response_type,
    fake_context,
    to_async,
)
from faststream.exceptions import SetupError

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import (
        AnyCallable,
        AsyncFunc,
        Lifespan,
        LoggerProto,
        SettingField,
    )
    from faststream._internal.broker.broker import BrokerUsecase


try:
    from pydantic import ValidationError as PValidation

    from faststream.exceptions import StartupValidationError

    @asynccontextmanager
    async def catch_startup_validation_error() -> AsyncIterator[None]:
        try:
            yield
        except PValidation as e:
            missed_fields = []
            invalid_fields = []
            for x in e.errors():
                location = str(x["loc"][0])
                if x["type"] == "missing":
                    missed_fields.append(location)
                else:
                    invalid_fields.append(location)

            raise StartupValidationError(
                missed_fields=missed_fields,
                invalid_fields=invalid_fields,
            ) from e

except ImportError:
    catch_startup_validation_error = fake_context


P_HookParams = ParamSpec("P_HookParams")
T_HookReturn = TypeVar("T_HookReturn")


class StartAbleApplication:
    def __init__(
        self,
        broker: Optional["BrokerUsecase[Any, Any]"] = None,
        /,
        provider: Optional["Provider"] = None,
        serializer: Optional["SerializerProto"] = EMPTY,
    ) -> None:
        self._init_setupable_(
            broker,
            provider=provider,
            serializer=serializer,
        )

    def _init_setupable_(  # noqa: PLW3201
        self,
        broker: Optional["BrokerUsecase[Any, Any]"] = None,
        /,
        provider: Optional["Provider"] = None,
        serializer: Optional["SerializerProto"] = EMPTY,
    ) -> None:
        self.context = ContextRepo()
        self.provider = provider or Provider()

        if serializer is EMPTY:
            from fast_depends.pydantic.serializer import PydanticSerializer

            serializer = PydanticSerializer()

        self._state: ApplicationState = BasicApplicationState(
            di_state=DIState(
                use_fastdepends=True,
                get_dependent=None,
                call_decorators=(),
                serializer=serializer,
                provider=self.provider,
                context=self.context,
            )
        )

        self.brokers = [broker] if broker else []

        self._setup()

    def _setup(self) -> None:
        for broker in self.brokers:
            broker._setup(OuterBrokerState(di_state=self._state.di_state))

    async def _start_broker(self) -> None:
        assert self.broker, "You should setup a broker"
        await self.broker.start()

    @property
    def broker(self) -> Optional["BrokerUsecase[Any, Any]"]:
        return self.brokers[0] if self.brokers else None

    def set_broker(self, broker: "BrokerUsecase[Any, Any]") -> None:
        """Set already existed App object broker.

        Useful then you create/init broker in `on_startup` hook.
        """
        if self.brokers:
            msg = f"`{self}` already has a broker. You can't use multiple brokers until 1.0.0 release."
            raise SetupError(msg)

        self.brokers.append(broker)
        self._setup()


class Application(StartAbleApplication):
    def __init__(
        self,
        broker: Optional["BrokerUsecase[Any, Any]"] = None,
        /,
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
            provider=provider,
            serializer=serializer,
        )

        self.context.set_global("app", self)

        self.logger = logger

        self._on_startup_calling: list[AsyncFunc] = [
            apply_types(to_async(x), context__=self.context) for x in on_startup
        ]
        self._after_startup_calling: list[AsyncFunc] = [
            apply_types(to_async(x), context__=self.context) for x in after_startup
        ]
        self._on_shutdown_calling: list[AsyncFunc] = [
            apply_types(to_async(x), context__=self.context) for x in on_shutdown
        ]
        self._after_shutdown_calling: list[AsyncFunc] = [
            apply_types(to_async(x), context__=self.context) for x in after_shutdown
        ]

        if lifespan is not None:
            self.lifespan_context = apply_types(
                func=lifespan,
                wrap_model=drop_response_type,
                context__=self.context,
            )
        else:
            self.lifespan_context = fake_context

    @property
    def running(self) -> bool:
        return self._state.running

    @abstractmethod
    def exit(self) -> None:
        """Stop application manually."""
        ...

    @abstractmethod
    async def run(
        self,
        log_level: int,
        run_extra_options: Optional[dict[str, "SettingField"]] = None,
    ) -> None: ...

    # Startup

    async def _startup(
        self,
        log_level: int = logging.INFO,
        run_extra_options: Optional[dict[str, "SettingField"]] = None,
    ) -> None:
        """Private method calls `start` with logging."""
        async with self._startup_logging(log_level=log_level):
            await self.start(**(run_extra_options or {}))

        self._state = RunningApplicationState(di_state=self._state.di_state)

    async def start(
        self,
        **run_extra_options: "SettingField",
    ) -> None:
        """Executes startup hooks and start broker."""
        async with self._start_hooks_context(**run_extra_options):
            await self._start_broker()

    @asynccontextmanager
    async def _start_hooks_context(
        self,
        **run_extra_options: "SettingField",
    ) -> AsyncIterator[None]:
        async with catch_startup_validation_error():
            for func in self._on_startup_calling:
                await func(**run_extra_options)

        yield

        for func in self._after_startup_calling:
            await func()

    @asynccontextmanager
    async def _startup_logging(
        self,
        log_level: int = logging.INFO,
    ) -> AsyncIterator[None]:
        """Separated startup logging."""
        self._log(
            log_level,
            "FastStream app starting...",
        )

        yield

        self._log(
            log_level,
            "FastStream app started successfully! To exit, press CTRL+C",
        )

    # Shutdown

    async def _shutdown(self, log_level: int = logging.INFO) -> None:
        """Private method calls `stop` with logging."""
        async with self._shutdown_logging(log_level=log_level):
            await self.stop()

        self._state = BasicApplicationState(di_state=self._state.di_state)

    async def stop(self) -> None:
        """Executes shutdown hooks and stop broker."""
        async with self._shutdown_hooks_context():
            for broker in self.brokers:
                await broker.close()

    @asynccontextmanager
    async def _shutdown_hooks_context(self) -> AsyncIterator[None]:
        for func in self._on_shutdown_calling:
            await func()

        yield

        for func in self._after_shutdown_calling:
            await func()

    @asynccontextmanager
    async def _shutdown_logging(
        self,
        log_level: int = logging.INFO,
    ) -> AsyncIterator[None]:
        """Separated startup logging."""
        self._log(
            log_level,
            "FastStream app shutting down...",
        )

        yield

        self._log(
            log_level,
            "FastStream app shut down gracefully.",
        )

    # Service methods

    def _log(self, level: int, message: str) -> None:
        if self.logger is not None:
            self.logger.log(level, message)

    # Hooks

    def on_startup(
        self,
        func: Callable[P_HookParams, T_HookReturn],
    ) -> Callable[P_HookParams, T_HookReturn]:
        """Add hook running BEFORE broker connected.

        This hook also takes an extra CLI options as a kwargs.
        """
        self._on_startup_calling.append(
            apply_types(to_async(func), context__=self.context)
        )
        return func

    def on_shutdown(
        self,
        func: Callable[P_HookParams, T_HookReturn],
    ) -> Callable[P_HookParams, T_HookReturn]:
        """Add hook running BEFORE broker disconnected."""
        self._on_shutdown_calling.append(
            apply_types(to_async(func), context__=self.context)
        )
        return func

    def after_startup(
        self,
        func: Callable[P_HookParams, T_HookReturn],
    ) -> Callable[P_HookParams, T_HookReturn]:
        """Add hook running AFTER broker connected."""
        self._after_startup_calling.append(
            apply_types(to_async(func), context__=self.context)
        )
        return func

    def after_shutdown(
        self,
        func: Callable[P_HookParams, T_HookReturn],
    ) -> Callable[P_HookParams, T_HookReturn]:
        """Add hook running AFTER broker disconnected."""
        self._after_shutdown_calling.append(
            apply_types(to_async(func), context__=self.context)
        )
        return func
