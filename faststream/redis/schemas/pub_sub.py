from copy import deepcopy

from faststream._internal.proto import NameRequired
from faststream._internal.utils.path import compile_path


class PubSub(NameRequired):
    """A class to represent a Redis PubSub channel."""

    channel_template: str
    """The channel as it was declared, e.g. `logs.{level}`."""

    name: str
    """The channel Redis is (p)subscribed to, e.g. `logs.*` for `logs.{level}`."""

    pattern: bool
    """Whether to subscribe with `psubscribe` rather than `subscribe`."""

    __slots__ = (
        "channel_template",
        "name",
        "path_regex",
        "pattern",
        "polling_interval",
    )

    def __init__(
        self,
        channel: str,
        pattern: bool = False,
        polling_interval: float = 1.0,
    ) -> None:
        reg, path = compile_path(
            channel,
            replace_symbol="*",
            patch_regex=lambda x: x.replace(r"\*", ".*"),
        )

        if reg is not None or "*" in channel:
            pattern = True

        super().__init__(path)

        self.path_regex = reg
        self.channel_template = channel
        self.pattern = pattern
        self.polling_interval = polling_interval

    def add_prefix(self, prefix: str) -> "PubSub":
        new_ch = deepcopy(self)
        new_ch.name = f"{prefix}{new_ch.name}"
        new_ch.channel_template = f"{prefix}{new_ch.channel_template}"
        return new_ch
