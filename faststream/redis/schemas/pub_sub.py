from faststream._internal.proto import NameRequired
from faststream._internal.utils.path import compile_path


class PubSub(NameRequired):
    """A class to represent a Redis PubSub channel."""

    __slots__ = (
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

        if reg is not None:
            pattern = True

        super().__init__(path)

        self.path_regex = reg
        self.pattern = channel if pattern else None
        self.polling_interval = polling_interval
