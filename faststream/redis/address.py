"""Reading a Redis address, once its Config value and Router prefix are known.

Redis names its addresses with value objects — `PubSub`, `ListSub`, `StreamSub` —
so a read here answers with one of those rather than with a string. Building one
is deferred to the read because the declaration site does not know enough: a
Config placeholder may still be standing in for the whole address (ADR-0001), and
it is inside these constructors that an Address template compiles (ADR-0004).
"""

from collections.abc import Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from faststream._internal.utils.path import PrefixedRead
from faststream.exceptions import SetupError
from faststream.redis.schemas import ListSub, PubSub, StreamSub

if TYPE_CHECKING:
    from faststream._internal.config_value import Configurable
    from faststream._internal.configs import BrokerConfig

AddressType = TypeVar("AddressType", PubSub, ListSub, StreamSub)

NO_BUILD_TIME_FIELDS: Mapping[str, Any] = MappingProxyType({})


class AddressRead(Generic[AddressType]):
    """A Redis address as its endpoint reads it: resolved first, then built.

    One read answers with the value object the endpoint talks through. The
    branch inside is the one ADR-0003 describes: a literal declaration is
    decorated with the Router prefix, a Config value reaches Redis exactly as
    it was supplied.
    """

    __slots__ = ("_built_as", "_declared", "_read", "_type")

    def __init__(
        self,
        declared: "Configurable[AddressType | str]",
        type_: type[AddressType],
        *,
        built_as: Mapping[str, Any] = NO_BUILD_TIME_FIELDS,
    ) -> None:
        """Hold a declared address until someone asks what it means.

        Args:
            declared: The address as written — a name, a value object, or a
                Config placeholder standing for either.
            type_: The value object this address is read as.
            built_as: The fields of `type_` that were consumed while the
                endpoint was constructed, and the values assumed for them. A
                resolved object disagreeing with any of them is refused.
        """
        self._declared: Configurable[AddressType | str] = declared
        self._type: type[AddressType] = type_
        self._built_as = built_as
        self._read: PrefixedRead[AddressType] = PrefixedRead()

    def read(self, config: "BrokerConfig") -> AddressType:
        """The address this endpoint talks to, kept once it has been built.

        Kept rather than re-derived because a Config value is fixed at
        `connect()` (ADR-0004), and re-keyed on the Router prefix, which is not
        settled until every `include_router` has run.
        """
        return self._read.read(
            config.prefix,
            lambda prefix: self._build(config, prefix),
        )

    def reset(self) -> None:
        """Forget the built address, so the next read builds it again.

        Undone with the connection the Config value was fixed for (ADR-0004).
        """
        self._read.reset()

    def config_key(self, config: "BrokerConfig") -> str | None:
        """The Config key this address was declared with, or `None` if literal.

        What an error message needs to name the placeholder a bad address came
        from, without reaching past the read layer for the declared option.
        """
        return config.config_key(self._declared)

    def _build(self, config: "BrokerConfig", prefix: str) -> AddressType:
        # Cast because `Configurable[T]` gives the type checker nothing to solve
        # `T` from once the option is the placeholder half of the union — the
        # same reason `resolve_address` casts.
        value = cast("AddressType | str", config.resolve_option(self._declared))

        # The branch is on how the option was *declared*, so a literal address
        # beside a placeholder is still decorated with the prefix (ADR-0003).
        if (key := config.config_key(self._declared)) is None:
            return self._type.validate(value).add_prefix(prefix)

        resolved = self._type.from_config_value(value, key)
        self._refuse_build_time_drift(resolved, key)
        return resolved

    def _refuse_build_time_drift(self, value: AddressType, config_key: str) -> None:
        """Refuse a resolved object that would have built a different endpoint.

        `batch`, and a Stream's consumer group, are read while the endpoint is
        being constructed: they pick which class is instantiated and which
        acknowledgement policy it carries. A Config value is not known then, so
        the endpoint is built as a plain name would build it — and an object
        arriving with anything else is refused here rather than left running
        against machinery chosen for a different shape (ADR-0002).
        """
        drifted = sorted(
            name
            for name, assumed in self._built_as.items()
            if getattr(value, name) != assumed
        )

        if not drifted:
            return

        fields = ", ".join(f"{name}={getattr(value, name)!r}" for name in drifted)
        msg = (
            f"Config value {config_key!r} resolved to a {type(value).__name__} with "
            f"{fields}, but that is read while the endpoint is built, before any "
            f"Config value is known. Declare it at the declaration site and let the "
            f"Config value name the address."
        )
        raise SetupError(msg)


def declared_batch(
    option: "Configurable[ListSub | StreamSub | str] | None",
) -> bool:
    """Whether the endpoint being built reads its address in batches.

    Decided while the endpoint is constructed, so a Config placeholder — which
    stands for the address and nothing else — reads as the plain name it will
    most often resolve to. `AddressRead` refuses a value that then disagrees.
    """
    return isinstance(option, ListSub | StreamSub) and option.batch
