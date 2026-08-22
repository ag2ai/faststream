from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, Optional, Union

from typing_extensions import TypeVar as TypeVar313

from faststream._internal.config_value import ConfigResolutionMixin, ConfigSource
from faststream._internal.constants import EMPTY
from faststream._internal.di import FastDependsConfig
from faststream._internal.logger import LoggerState
from faststream._internal.producer import ProducerProto, ProducerUnset
from faststream._internal.types import IdGenerator
from faststream.message import gen_cor_id

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.parser import CodecProto
    from faststream._internal.types import BrokerMiddleware, CustomCallable
    from faststream.middlewares import AckPolicy


@dataclass(kw_only=True)
class BrokerConfig(ConfigResolutionMixin):
    prefix: str = ""
    include_in_schema: bool | None = True

    # The Config value store. Read it through `resolve_option`, never directly:
    # a single record sees one level only, and levels have to compose.
    config_values: ConfigSource = None

    broker_middlewares: Sequence["BrokerMiddleware[Any]"] = ()
    broker_parser: Optional["CustomCallable"] = None
    broker_decoder: Optional["CustomCallable"] = None
    broker_codec: Optional["CodecProto"] = None

    producer: "ProducerProto[Any]" = field(default_factory=ProducerUnset)
    logger: "LoggerState" = field(default_factory=LoggerState)
    fd_config: "FastDependsConfig" = field(default_factory=FastDependsConfig)
    id_generator: IdGenerator = gen_cor_id

    # subscriber options
    broker_dependencies: Iterable["Dependant"] = ()
    graceful_timeout: float | None = None
    ack_policy: "AckPolicy" = field(default_factory=lambda: EMPTY)
    extra_context: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id: {id(self)})"

    @property
    def config_sources(self) -> Iterator[ConfigSource]:
        yield self.config_values

    def __bool__(self) -> bool:
        return bool(
            self.include_in_schema is not None
            or self.broker_middlewares
            or self.broker_dependencies
            or self.prefix,
        )

    def add_middleware(self, middleware: "BrokerMiddleware[Any]") -> None:
        self.broker_middlewares = (*self.broker_middlewares, middleware)

    def insert_middleware(self, middleware: "BrokerMiddleware[Any]") -> None:
        self.broker_middlewares = (middleware, *self.broker_middlewares)


BrokerConfigType = TypeVar313(
    "BrokerConfigType",
    bound=BrokerConfig,
    default=BrokerConfig,
)

ConfigType = Union["ConfigComposition[Any]", "BrokerConfigType", BrokerConfig]


class ConfigComposition(ConfigResolutionMixin, Generic[BrokerConfigType]):  # noqa: PLR0904
    def __init__(self, config: BrokerConfigType) -> None:
        self.__own_config = config
        self.configs: tuple[ConfigType, ...] = (config,)

        # Config values of the test broker wrapping this one, which beat every
        # level below. `None` when there is no test broker, and skipped as a
        # source then.
        self.config_values_override: ConfigSource = None

    @property
    def broker_config(self) -> "BrokerConfigType":
        assert self.configs
        return self.configs[0]  # type: ignore[return-value]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({', '.join(repr(c) for c in self.configs)})"

    def add_config(self, config: "ConfigType") -> None:
        self.configs = (config, *self.configs)

    def add_outer_config(self, config: "BrokerConfig") -> None:
        """Compose the record of an outer application into this composition.

        Appended rather than prepended, so everything already composed here —
        the Broker's own record first of all — keeps winning over it.
        """
        self.fd_config = config.fd_config | self.fd_config
        self.configs = (*self.configs, config)

    def reset(self) -> None:
        self.configs = (self.__own_config,)

    # broker priority options
    @property
    def producer(self) -> "ProducerProto[Any]":
        return self.broker_config.producer

    @property
    def logger(self) -> "LoggerState":
        return self.broker_config.logger

    @property
    def fd_config(self) -> "FastDependsConfig":
        return self.broker_config.fd_config

    @fd_config.setter
    def fd_config(self, value: "FastDependsConfig") -> None:
        self.broker_config.fd_config = value

    @property
    def graceful_timeout(self) -> float | None:
        return self.broker_config.graceful_timeout

    @property
    def id_generator(self) -> IdGenerator:
        return self.broker_config.id_generator

    def add_middleware(self, middleware: "BrokerMiddleware[Any]") -> None:
        self.broker_config.add_middleware(middleware)

    def insert_middleware(self, middleware: "BrokerMiddleware[Any]") -> None:
        self.broker_config.insert_middleware(middleware)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.broker_config, name)

    # first valuable option
    @property
    def broker_parser(self) -> Optional["CustomCallable"]:
        for c in self.configs:
            if c.broker_parser:
                return c.broker_parser
        return None

    @property
    def broker_decoder(self) -> Optional["CustomCallable"]:
        for c in self.configs:
            if c.broker_decoder:
                return c.broker_decoder
        return None

    @property
    def broker_codec(self) -> Optional["CodecProto"]:
        for c in self.configs:
            if c.broker_codec:
                return c.broker_codec
        return None

    @property
    def ack_policy(self) -> "AckPolicy":
        for c in reversed(self.configs):
            ack = c.ack_policy
            if ack is not EMPTY:
                return ack
        return EMPTY  # type: ignore[no-any-return]

    # merged options
    @property
    def config_sources(self) -> Iterator[ConfigSource]:
        yield self.config_values_override
        for c in self.configs:
            yield from c.config_sources

    @property
    def extra_context(self) -> dict[str, Any]:
        context: dict[str, Any] = {}
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
    def broker_middlewares(self) -> Sequence["BrokerMiddleware[Any]"]:
        return [m for c in self.configs for m in c.broker_middlewares]

    @property
    def broker_dependencies(self) -> Iterable["Dependant"]:
        return (b for c in self.configs for b in c.broker_dependencies)
