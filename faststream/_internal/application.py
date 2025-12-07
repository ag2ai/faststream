import logging
from abc import abstractmethod
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Optional, TypeVar

from typing_extensions import ParamSpec

from faststream._internal.di import FastDependsConfig
from faststream._internal.logger import logger
from faststream._internal.utils import apply_types
from faststream._internal.utils.functions import fake_context, to_async
from faststream.exceptions import SetupError
from faststream.specification import AsyncAPI

if TYPE_CHECKING:
    from faststream._internal.basic_types import (
        AnyCallable,
        AsyncFunc,
        Lifespan,
        LoggerProto,
        SettingField,
    )
    from faststream._internal.broker import BrokerUsecase
    from faststream._internal.context import ContextRepo
    from faststream.specification.base import SpecificationFactory


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
        specification: Optional["SpecificationFactory"] = None,
        config: Optional["FastDependsConfig"] = None,
    ) -> None:
        self._init_setupable_(
            broker,
            config=config,
            specification=specification,
        )

    @property
    def context(self) -> "ContextRepo":
        return self.config.context

    def _init_setupable_(  # noqa: PLW3201
        self,
        broker: Optional["BrokerUsecase[Any, Any]"] = None,
        /,
        specification: Optional["SpecificationFactory"] = None,
        config: Optional["FastDependsConfig"] = None,
    ) -> None:
        self.config = config or FastDependsConfig()
        self.config.context.set_global("app", self)
        self.brokers: list[BrokerUsecase[Any, Any]] = []

        self.schema: SpecificationFactory = specification or AsyncAPI()

        if broker:
            self._add_broker(broker)

    async def _start_broker(self) -> None:
        assert self.brokers, "You should setup a broker"
        for b in self.brokers:
            await b.start()

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
        self._add_broker(broker)

    def _add_broker(self, broker: "BrokerUsecase[Any, Any]") -> None:
        if broker in self.brokers:
            msg = f"Broker {broker} is already added"
            raise SetupError(msg)
        self.brokers.append(broker)
        self.schema.add_broker(broker)
        broker._update_fd_config(self.config)


class Application(StartAbleApplication):
    def __init__(
        self,
        broker: Optional["BrokerUsecase[Any, Any]"] = None,
        /,
        config: Optional["FastDependsConfig"] = None,
        logger: Optional["LoggerProto"] = logger,
        lifespan: Optional["Lifespan"] = None,
        on_startup: Sequence["AnyCallable"] = (),
        after_startup: Sequence["AnyCallable"] = (),
        on_shutdown: Sequence["AnyCallable"] = (),
        after_shutdown: Sequence["AnyCallable"] = (),
        specification: Optional["SpecificationFactory"] = None,
    ) -> None:
        self.logger = logger

        super().__init__(broker, config=config, specification=specification)

        self._on_startup_calling: list[AsyncFunc] = [
            apply_types(
                to_async(x),
                serializer_cls=self.config._serializer,
                context__=self.context,
            )
            for x in on_startup
        ]
        self._after_startup_calling: list[AsyncFunc] = [
            apply_types(
                to_async(x),
                serializer_cls=self.config._serializer,
                context__=self.context,
            )
            for x in after_startup
        ]
        self._on_shutdown_calling: list[AsyncFunc] = [
            apply_types(
                to_async(x),
                serializer_cls=self.config._serializer,
                context__=self.context,
            )
            for x in on_shutdown
        ]
        self._after_shutdown_calling: list[AsyncFunc] = [
            apply_types(
                to_async(x),
                serializer_cls=self.config._serializer,
                context__=self.context,
            )
            for x in after_shutdown
        ]

        if lifespan:
            self.lifespan_context = apply_types(
                func=lifespan,
                serializer_cls=self.config._serializer,
                cast_result=False,
                context__=self.context,
            )
        else:
            self.lifespan_context = fake_context

    @abstractmethod
    def exit(self) -> None:
        """Stop application manually."""
        ...

    @abstractmethod
    async def run(
        self,
        log_level: int,
        run_extra_options: dict[str, "SettingField"] | None = None,
    ) -> None: ...

    # Startup

    async def _startup(
        self,
        log_level: int = logging.INFO,
        run_extra_options: dict[str, "SettingField"] | None = None,
    ) -> None:
        """Private method calls `start` with logging."""
        async with self._startup_logging(log_level=log_level):
            await self.start(**(run_extra_options or {}))

        self.running = True

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

        self.running = False

    async def stop(self) -> None:
        """Executes shutdown hooks and stop broker."""
        async with self._shutdown_hooks_context():
            for broker in self.brokers:
                await broker.stop()

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
        self._log(log_level, "FastStream app shutting down...")

        yield

        self._log(log_level, "FastStream app shut down gracefully.")

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
            apply_types(
                to_async(func),
                serializer_cls=self.config._serializer,
                context__=self.context,
            ),
        )
        return func

    def on_shutdown(
        self,
        func: Callable[P_HookParams, T_HookReturn],
    ) -> Callable[P_HookParams, T_HookReturn]:
        """Add hook running BEFORE broker disconnected."""
        self._on_shutdown_calling.append(
            apply_types(
                to_async(func),
                serializer_cls=self.config._serializer,
                context__=self.context,
            ),
        )
        return func

    def after_startup(
        self,
        func: Callable[P_HookParams, T_HookReturn],
    ) -> Callable[P_HookParams, T_HookReturn]:
        """Add hook running AFTER broker connected."""
        self._after_startup_calling.append(
            apply_types(
                to_async(func),
                serializer_cls=self.config._serializer,
                context__=self.context,
            ),
        )
        return func

    def after_shutdown(
        self,
        func: Callable[P_HookParams, T_HookReturn],
    ) -> Callable[P_HookParams, T_HookReturn]:
        """Add hook running AFTER broker disconnected."""
        self._after_shutdown_calling.append(
            apply_types(
                to_async(func),
                serializer_cls=self.config._serializer,
                context__=self.context,
            ),
        )
        return func
