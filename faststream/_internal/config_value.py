"""Config values — user-supplied values filling in endpoint options.

Not to be confused with the options objects in `faststream._internal.configs`,
which record how a Broker, Subscriber or Publisher was constructed.
"""

from collections.abc import Iterator, Mapping
from typing import Any, TypeAlias, TypeVar, cast

from faststream._internal.constants import EMPTY
from faststream.exceptions import SetupError

T = TypeVar("T")

ConfigSource: TypeAlias = object
"""A Mapping or an arbitrary object (a settings instance, for example) holding Config values.

Deliberately `object` rather than `Any`: any object really is a valid source, and
`Any` would silence unrelated type errors wherever a source is passed around.
"""


class Config:
    """A placeholder for a value supplied at the Broker or the App level.

    Written where an address would go, it stands for a value that is not known
    at declaration time::

        @router.subscriber(Config("IN_TOPIC"))
        async def handler(msg: str) -> None: ...

        broker = KafkaBroker(config={"IN_TOPIC": "orders"})
        broker.include_router(router)

    Args:
        key: Name of the Config value to read.
        default:
            Value to use when the key is absent everywhere. Omit it to make a
            missing key an error; `None` is a real default, not an omission.
    """

    __slots__ = ("default", "key")

    def __init__(self, key: str, default: Any = EMPTY) -> None:
        self.key = key
        self.default = default

    def __repr__(self) -> str:
        if self.default is EMPTY:
            return f"{self.__class__.__name__}({self.key!r})"
        return f"{self.__class__.__name__}({self.key!r}, default={self.default!r})"


Configurable: TypeAlias = T | Config
"""An option that accepts either a literal value or a `Config` placeholder.

`Configurable[str]` reads as "a string, or a Config value standing in for one",
and keeps every declaration site from spelling the union out by hand.
"""


def lookup_config_value(source: "ConfigSource", key: str) -> Any:
    """Read `key` out of a single source, falling back from item to attribute access.

    Returns EMPTY when the source has nothing under that key.
    """
    if source is None:
        return EMPTY

    try:
        return cast("Mapping[str, Any]", source)[key]
    except LookupError:
        # Item access worked and the source has nothing under that key. Falling back
        # to attributes here would resolve `Config("items")` to `dict.items`.
        return EMPTY
    except TypeError:
        # Not subscriptable: a settings object rather than a mapping.
        return getattr(source, key, EMPTY)


class ConfigResolutionMixin:
    """Resolves Config placeholders against the Config values in scope.

    The one place where level precedence, defaults and the missing-key error
    live, so that six brokers do not grow six versions of them.
    """

    @property
    def prefix(self) -> str:
        """The Router prefix in scope, which literal addresses are decorated with."""
        raise NotImplementedError

    @property
    def config_sources(self) -> Iterator["ConfigSource"]:
        """Every Config value source in scope, most specific first."""
        raise NotImplementedError

    def resolve_address(self, option: Configurable[str]) -> str:
        """Return the address to use on the infrastructure broker.

        A Config value reaches the broker exactly as supplied; the Router prefix
        decorates literal declarations only. The branch is on how the option was
        *declared*, so a literal address beside a placeholder is still prefixed.
        See ADR-0003.
        """
        if isinstance(option, Config):
            return cast("str", self.resolve_option(option))

        return f"{self.prefix}{option}"

    def resolve_option(self, option: Configurable[T]) -> T:
        """Return `option` itself, or the Config value it stands for."""
        if not isinstance(option, Config):
            return option

        for source in self.config_sources:
            value = lookup_config_value(source, option.key)
            if value is not EMPTY:
                return cast("T", value)

        if option.default is not EMPTY:
            return cast("T", option.default)

        msg = (
            f"Config value {option.key!r} is not found. "
            f"Pass it as `config={{{option.key!r}: ...}}` to your broker or application."
        )
        raise SetupError(msg)
