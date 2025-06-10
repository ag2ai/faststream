from copy import deepcopy

from faststream._internal.proto import NameRequired
from faststream.exceptions import SetupError


class StreamSub(NameRequired):
    """A class to represent a Redis Stream subscriber."""

    __slots__ = (
        "batch",
        "consumer",
        "group",
        "last_id",
        "max_records",
        "maxlen",
        "name",
        "no_ack",
        "polling_interval",
    )

    def __init__(
        self,
        stream: str,
        polling_interval: int | None = 100,
        group: str | None = None,
        consumer: str | None = None,
        batch: bool = False,
        no_ack: bool = False,
        last_id: str | None = None,
        maxlen: int | None = None,
        max_records: int | None = None,
    ) -> None:
        if (group and not consumer) or (not group and consumer):
            msg = "You should specify `group` and `consumer` both"
            raise SetupError(msg)

        if last_id is None:
            last_id = "$"

        super().__init__(stream)

        self.group = group
        self.consumer = consumer
        self.polling_interval = polling_interval
        self.batch = batch
        self.no_ack = no_ack
        self.last_id = last_id
        self.maxlen = maxlen
        self.max_records = max_records

    def add_prefix(self, prefix: str) -> "StreamSub":
        new_stream = deepcopy(self)
        new_stream.name = f"{prefix}{new_stream.name}"
        return new_stream
