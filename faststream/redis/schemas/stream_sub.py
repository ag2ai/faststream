import warnings
from copy import deepcopy

from faststream._internal.proto import NameRequired
from faststream.exceptions import SetupError


class StreamSub(NameRequired):
    """A class to represent a Redis Stream subscriber.

    Args:
        batch:
            Whether to consume messages in batches or not.
        max_records:
            Number of messages to consume as one batch.
        consumer:
            The consumer unique name

            https://redis.io/docs/latest/develop/tools/insight/tutorials/insight-stream-consumer/#run-the-consumer
        group:
            The name of consumer group
        last_id:
            An Entry ID, which uses to pick up from where it left off after it is restarted.
        maxlen:
            Redis Stream maxlen publish option. Remove eldest message if maxlen exceeded.

            https://redis.io/docs/latest/develop/data-types/streams/#capped-streams
        name:
            The original Redis Stream name.
        no_ack:
            If True, to enable the XREADGROUP NOACK subcommand.

            https://redis.io/docs/latest/commands/xreadgroup/#differences-between-xread-and-xreadgroup
        polling_interval:
            Polling interval in seconds.
    """

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
        polling_interval: int | None = None,
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

        if group and consumer:
            if last_id != ">":
                if polling_interval:
                    warnings.warn(
                        message="`polling_interval` is not supported by consumer group with last_id other than `>`",
                        category=RuntimeWarning,
                        stacklevel=1,
                    )

                if no_ack:
                    warnings.warn(
                        message="`no_ack` is not supported by consumer group with last_id other than `>`",
                        category=RuntimeWarning,
                        stacklevel=1,
                    )

            elif no_ack:
                warnings.warn(
                    message="`no_ack` has no effect with consumer group",
                    category=RuntimeWarning,
                    stacklevel=1,
                )

        if last_id is None:
            last_id = ">" if group and consumer else "$"

        super().__init__(stream)

        self.group = group
        self.consumer = consumer
        self.polling_interval = polling_interval or 100
        self.batch = batch
        self.no_ack = no_ack
        self.last_id = last_id
        self.maxlen = maxlen
        self.max_records = max_records

    def add_prefix(self, prefix: str) -> "StreamSub":
        new_stream = deepcopy(self)
        new_stream.name = f"{prefix}{new_stream.name}"
        return new_stream
