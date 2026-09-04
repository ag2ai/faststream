import warnings
from copy import deepcopy

from faststream._internal.proto import NameRequired
from faststream.exceptions import SetupError
from faststream.redis._compat import REDIS_V710, _REDIS_VERSION


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
            Polling interval in milliseconds.
        min_idle_time:
            Minimum idle time in milliseconds for a message to be eligible for claiming via XAUTOCLAIM.
            Messages that have been pending (unacknowledged) for at least this duration can be
            reclaimed by this consumer. Only applicable when using consumer groups.

            https://redis.io/docs/latest/commands/xautoclaim/
        declare:
            Whether to create the stream when creating a consumer group.
        claim_min_idle_time:
            Redis 8.4+ `XREADGROUP CLAIM` option (milliseconds). When set, each read
            also claims messages of the same group that have been pending for at least
            this duration, so a single subscriber consumes new messages and recovers
            abandoned ones in one command. Claimed entries arrive first; per-entry
            metadata is exposed via the `idle_times` / `delivery_counts` keys of the
            raw message. Requires redis-py 7.1.0+ and Redis server 8.4+ (older servers
            reject the CLAIM option). Mutually exclusive with `min_idle_time` and
            `no_ack`.

            https://redis.io/docs/latest/commands/xreadgroup/#the-claim-option
    """

    __slots__ = (
        "batch",
        "claim_min_idle_time",
        "consumer",
        "declare",
        "group",
        "last_id",
        "max_records",
        "maxlen",
        "min_idle_time",
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
        min_idle_time: int | None = None,
        declare: bool = True,
        claim_min_idle_time: int | None = None,
    ) -> None:
        if (group and not consumer) or (not group and consumer):
            msg = "You should specify `group` and `consumer` both"
            raise SetupError(msg)

        if not declare and not group:
            warnings.warn(
                message="`declare` has no effect without consumer group",
                category=RuntimeWarning,
                stacklevel=1,
            )

        if last_id is None:
            last_id = ">" if group and consumer else "$"

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

        if claim_min_idle_time is not None:
            if not REDIS_V710:
                msg = (
                    "`claim_min_idle_time` requires redis-py 7.1.0 or newer "
                    f"(installed: {_REDIS_VERSION})"
                )
                raise SetupError(msg)

            if min_idle_time is not None:
                msg = (
                    "`claim_min_idle_time` (XREADGROUP CLAIM) and `min_idle_time` "
                    "(XAUTOCLAIM) are mutually exclusive"
                )
                raise SetupError(msg)

            if no_ack:
                msg = (
                    "`claim_min_idle_time` with `no_ack` causes infinite "
                    "redelivery: claimed entries stay in the PEL and are "
                    "never acknowledged"
                )
                raise SetupError(msg)

            if not group:
                msg = "`claim_min_idle_time` requires `group` and `consumer`"
                raise SetupError(msg)

            if last_id != ">":
                msg = (
                    "`claim_min_idle_time` requires last_id `>`: Redis ignores "
                    "the CLAIM option for any other id"
                )
                raise SetupError(msg)

        super().__init__(stream)

        self.group = group
        self.consumer = consumer
        self.declare = declare
        self.polling_interval = polling_interval or 100
        self.batch = batch
        self.no_ack = no_ack
        self.last_id = last_id
        self.maxlen = maxlen
        self.max_records = max_records
        self.min_idle_time = min_idle_time
        self.claim_min_idle_time = claim_min_idle_time

    def add_prefix(self, prefix: str) -> "StreamSub":
        new_stream = deepcopy(self)
        new_stream.name = f"{prefix}{new_stream.name}"
        return new_stream
