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
    ) -> None:
        address = Address(channel, REDIS_ADDRESS_SYNTAX)

        if address.regex is not None or "*" in channel:
            pattern = True

        # `name` is `NameRequired`'s writeable contract, so it mirrors the Broker
        # address rather than deriving from it. Both build points keep it in step.
        super().__init__(address.broker_address)

        self.address = address
        self.pattern = pattern
        self.polling_interval = polling_interval

    @property
    def path_regex(self) -> Pattern[str] | None:
        return self.address.regex

    def add_prefix(self, prefix: str) -> "PubSub":
        new_ch = deepcopy(self)
        new_ch.address = new_ch.address.add_prefix(prefix)
        new_ch.name = new_ch.address.broker_address
        return new_ch
