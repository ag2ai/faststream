from copy import deepcopy
from re import Pattern

from faststream._internal.proto import NameRequired
from faststream._internal.utils.path import Address, AddressSyntax

REDIS_ADDRESS_SYNTAX = AddressSyntax(
    replace_symbol="*",
    patch_regex=lambda x: x.replace(r"\*", ".*"),
)


class PubSub(NameRequired):
    """A class to represent a Redis PubSub channel."""

    address: Address
    """The channel this endpoint was declared with, and its Broker address."""

    name: str
    """The channel Redis is (p)subscribed to, e.g. `logs.*` for `logs.{level}`."""

    pattern: bool
    """Whether to subscribe with `psubscribe` rather than `subscribe`."""

    __slots__ = (
        "address",
        "name",
        "pattern",
        "polling_interval",
    )

    def __init__(
        self,
        channel: str,
        pattern: bool = False,
        polling_interval: float = 1.0,
        config_key: str | None = None,
    ) -> None:
        address = Address(channel, REDIS_ADDRESS_SYNTAX, config_key)

        if address.regex is not None or "*" in channel:
            pattern = True

        # `name` is `NameRequired`'s writeable contract, so it mirrors the Broker
        # address rather than deriving from it. Both build points keep it in step.
        super().__init__(address.broker_address)

        self.address = address
        self.pattern = pattern
        self.polling_interval = polling_interval

    @classmethod
    def from_config_value(cls, value: "PubSub | str", config_key: str) -> "PubSub":
        """Build this channel out of a Config value, naming the value it came from.

        The `Address` compiles inside the constructor, so the key has to arrive
        before that rather than be attached after: a resolved value whose braces
        do not spell out Path parameters fails right there, and the failure has
        to be able to name what to fix.
        """
        if isinstance(value, str):
            return cls(value, config_key=config_key)

        # A copy, because the caller's object is theirs: the same prepared
        # `PubSub` may be supplied as one Config value and used literally
        # elsewhere, and stamping it in place would make the literal's failure
        # name a Config key it never came from.
        stamped = deepcopy(value)
        stamped.address.config_key = config_key
        return stamped

    @property
    def path_regex(self) -> Pattern[str] | None:
        return self.address.regex

    def add_prefix(self, prefix: str) -> "PubSub":
        new_ch = deepcopy(self)
        new_ch.address = new_ch.address.add_prefix(prefix)
        new_ch.name = new_ch.address.broker_address
        return new_ch
