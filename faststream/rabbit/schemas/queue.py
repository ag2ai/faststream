from copy import deepcopy
from enum import Enum
from typing import TYPE_CHECKING, Literal, Optional, TypedDict, Union, overload

from faststream._internal.constants import EMPTY
from faststream._internal.proto import NameRequired
from faststream._internal.utils.path import compile_path
from faststream.exceptions import SetupError

if TYPE_CHECKING:
    from aio_pika.abc import TimeoutType

    from faststream._internal.basic_types import AnyDict


class QueueType(str, Enum):
    """Queue types for RabbitMQ.

    Enum should be lowercase to match RabbitMQ API.
    """

    CLASSIC = "classic"
    QUORUM = "quorum"
    STREAM = "stream"


class RabbitQueue(NameRequired):
    """A class to represent a RabbitMQ queue.

    You can find information about all options in the official RabbitMQ documentation:

    https://www.rabbitmq.com/docs/queues
    """

    __slots__ = (
        "arguments",
        "auto_delete",
        "bind_arguments",
        "durable",
        "exclusive",
        "name",
        "passive",
        "path_regex",
        "robust",
        "routing_key",
        "timeout",
    )

    def __repr__(self) -> str:
        if self.passive:
            body = ""
        else:
            body = f", robust={self.robust}, durable={self.durable}, exclusive={self.exclusive}, auto_delete={self.auto_delete})"

        return f"{self.__class__.__name__}({self.name}, routing_key='{self.routing}'{body})"

    def __hash__(self) -> int:
        """Supports hash to store real objects in declarer."""
        return sum(
            (
                hash(self.name),
                int(self.durable),
                int(self.exclusive),
                int(self.auto_delete),
            ),
        )

    @property
    def routing(self) -> str:
        """Return real routing_key of object."""
        return self.routing_key or self.name

    @overload
    def __init__(
        self,
        name: str,
        queue_type: Literal[QueueType.CLASSIC] = QueueType.CLASSIC,
        durable: bool = EMPTY,
        exclusive: bool = False,
        passive: bool = False,
        auto_delete: bool = False,
        arguments: Optional["ClassicQueueArgs"] = None,
        timeout: "TimeoutType" = None,
        robust: bool = True,
        bind_arguments: Optional["AnyDict"] = None,
        routing_key: str = "",
    ) -> None: ...

    @overload
    def __init__(
        self,
        name: str,
        queue_type: Literal[QueueType.QUORUM],
        durable: Literal[True],
        exclusive: bool = False,
        passive: bool = False,
        auto_delete: bool = False,
        arguments: Optional["QuorumQueueArgs"] = None,
        timeout: "TimeoutType" = None,
        robust: bool = True,
        bind_arguments: Optional["AnyDict"] = None,
        routing_key: str = "",
    ) -> None: ...

    @overload
    def __init__(
        self,
        name: str,
        queue_type: Literal[QueueType.STREAM],
        durable: Literal[True],
        exclusive: bool = False,
        passive: bool = False,
        auto_delete: bool = False,
        arguments: Optional["StreamQueueArgs"] = None,
        timeout: "TimeoutType" = None,
        robust: bool = True,
        bind_arguments: Optional["AnyDict"] = None,
        routing_key: str = "",
    ) -> None: ...

    def __init__(
        self,
        name: str,
        queue_type: QueueType = QueueType.CLASSIC,
        durable: bool = EMPTY,
        exclusive: bool = False,
        passive: bool = False,
        auto_delete: bool = False,
        arguments: Union[
            "QuorumQueueArgs",
            "ClassicQueueArgs",
            "StreamQueueArgs",
            "AnyDict",
            None,
        ] = None,
        timeout: "TimeoutType" = None,
        robust: bool = True,
        bind_arguments: Optional["AnyDict"] = None,
        routing_key: str = "",
    ) -> None:
        """Initialize the RabbitMQ queue.

        :param name: RabbitMQ queue name.
        :param durable: Whether the object is durable.
        :param exclusive: The queue can be used only in the current connection and will be deleted after connection closed.
        :param passive: Do not create queue automatically.
        :param auto_delete: The queue will be deleted after connection closed.
        :param arguments: Queue declaration arguments.
                          You can find information about them in the official RabbitMQ documentation:
                          https://www.rabbitmq.com/docs/queues#optional-arguments
        :param timeout: Send confirmation time from RabbitMQ.
        :param robust: Whether to declare queue object as restorable.
        :param bind_arguments: Queue-exchange binding options.
        :param routing_key: Explicit binding routing key. Uses name if not presented.
        """
        re, routing_key = compile_path(
            routing_key,
            replace_symbol="*",
            patch_regex=lambda x: x.replace(r"\#", ".+"),
        )

        if queue_type is QueueType.QUORUM or queue_type is QueueType.STREAM:
            if durable is EMPTY:
                durable = True
            elif not durable:
                error_msg = "Quorum and Stream queues must be durable"
                raise SetupError(error_msg)
        elif durable is EMPTY:
            durable = False

        super().__init__(name)

        self.path_regex = re
        self.durable = durable
        self.exclusive = exclusive
        self.bind_arguments = bind_arguments
        self.routing_key = routing_key
        self.robust = robust
        self.passive = passive
        self.auto_delete = auto_delete
        self.arguments = {"x-queue-type": queue_type.value, **(arguments or {})}
        self.timeout = timeout

    def add_prefix(self, prefix: str) -> "RabbitQueue":
        new_q: RabbitQueue = deepcopy(self)

        new_q.name = f"{prefix}{new_q.name}"

        if new_q.routing_key:
            new_q.routing_key = f"{prefix}{new_q.routing_key}"

        return new_q


CommonQueueArgs = TypedDict(
    "CommonQueueArgs",
    {
        "x-queue-leader-locator": Literal["client-local", "balanced"],
        "x-max-length-bytes": int,
    },
    total=False,
)

SharedQueueClassicAndQuorumArgs = TypedDict(
    "SharedQueueClassicAndQuorumArgs",
    {
        "x-expires": int,
        "x-message-ttl": int,
        "x-single-active-consumer": bool,
        "x-dead-letter-exchange": str,
        "x-dead-letter-routing-key": str,
        "x-max-length": int,
        "x-max-priority": int,
    },
    total=False,
)


QueueClassicTypeSpecificArgs = TypedDict(
    "QueueClassicTypeSpecificArgs",
    {"x-overflow": Literal["drop-head", "reject-publish", "reject-publish-dlx"]},
    total=False,
)

QueueQuorumTypeSpecificArgs = TypedDict(
    "QueueQuorumTypeSpecificArgs",
    {
        "x-overflow": Literal["drop-head", "reject-publish"],
        "x-delivery-limit": int,
        "x-quorum-initial-group-size": int,
        "x-quorum-target-group-size": int,
        "x-dead-letter-strategy": Literal["at-most-once", "at-least-once"],
    },
    total=False,
)


QueueStreamTypeSpecificArgs = TypedDict(
    "QueueStreamTypeSpecificArgs",
    {
        "x-max-age": str,
        "x-stream-max-segment-size-bytes": int,
        "x-stream-filter-size-bytes": int,
        "x-initial-cluster-size": int,
    },
    total=False,
)


class StreamQueueArgs(CommonQueueArgs, QueueStreamTypeSpecificArgs):
    pass


class ClassicQueueArgs(
    CommonQueueArgs, SharedQueueClassicAndQuorumArgs, QueueClassicTypeSpecificArgs
):
    pass


class QuorumQueueArgs(
    CommonQueueArgs, SharedQueueClassicAndQuorumArgs, QueueQuorumTypeSpecificArgs
):
    pass
