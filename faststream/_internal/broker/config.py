from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from faststream._internal.di import FastDependsConfig
from faststream._internal.logger import LoggerState
from faststream._internal.producer import ProducerProto, ProducerUnset

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import AnyDict
    from faststream._internal.types import AsyncCallable, BrokerMiddleware


@dataclass(kw_only=True)
class BrokerConfig:
    prefix: str = ""
    include_in_schema: Optional[bool] = True

    broker_middlewares: Iterable["BrokerMiddleware[Any]"] = ()
    broker_parser: Optional["AsyncCallable"] = None
    broker_decoder: Optional["AsyncCallable"] = None

    producer: "ProducerProto" = field(default_factory=ProducerUnset)
    logger: "LoggerState" = field(default_factory=LoggerState)
    fd_config: "FastDependsConfig" = field(default_factory=FastDependsConfig)

    # subscriber options
    broker_dependencies: Iterable["Dependant"] = ()
    graceful_timeout: Optional[float] = None
    extra_context: "AnyDict" = field(default_factory=dict)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id: {id(self)})"

    def __bool__(self) -> bool:
        return bool(
            self.include_in_schema is not None
            or self.broker_middlewares
            or self.broker_dependencies
            or self.prefix
        )

    def add_middleware(self, middleware: "BrokerMiddleware[Any]") -> None:
        self.broker_middlewares = (*self.broker_middlewares, middleware)


class ConfigComposition:
    def __init__(self, *configs: "BrokerConfig") -> None:
        self.configs = configs

    @property
    def broker_config(self) -> "BrokerConfig":
        assert self.configs
        return self.configs[0]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({', '.join(repr(c) for c in self.configs)})"

    def add_config(self, config: "BrokerConfig") -> None:
        self.configs = (config, *self.configs)

    # broker priorrity options
    @property
    def producer(self) -> "ProducerProto":
        return self.broker_config.producer

    @property
    def logger(self) -> "LoggerState":
        return self.broker_config.logger

    @property
    def fd_config(self) -> "FastDependsConfig":
        return self.broker_config.fd_config

    @property
    def graceful_timeout(self) -> Optional[float]:
        return self.broker_config.graceful_timeout

    def __getattr__(self, name: str) -> Any:
        return getattr(self.broker_config, name)

    # first valuable option
    @property
    def broker_parser(self) -> Optional["AsyncCallable"]:
        for c in self.configs:
            if c.broker_parser:
                return c.broker_parser
        return None

    @property
    def broker_decoder(self) -> Optional["AsyncCallable"]:
        for c in self.configs:
            if c.broker_decoder:
                return c.broker_decoder
        return None

    # merged options
    @property
    def extra_context(self) -> "AnyDict":
        context: AnyDict = {}
        for c in self.configs:
            context |= c.extra_context
        return context

    @property
    def prefix(self) -> str:
        return "".join(c.prefix for c in self.configs)

    @property
    def include_in_schema(self) -> bool:
        return all(c.include_in_schema is not False for c in self.configs)

    @property
    def broker_middlewares(self) -> Iterable["BrokerMiddleware[Any]"]:
        return [m for c in self.configs for m in c.broker_middlewares]

    @property
    def broker_dependencies(self) -> Iterable["Dependant"]:
        return [b for c in self.configs for b in c.broker_dependencies]
