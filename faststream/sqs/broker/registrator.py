from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from typing_extensions import override

from faststream._internal.broker.registrator import Registrator
from faststream._internal.constants import EMPTY
from faststream.exceptions import SetupError
from faststream.middlewares import AckPolicy
from faststream.sqs.configs import SQSBrokerConfig
from faststream.sqs.message import SQSRawMessage
from faststream.sqs.publisher.factory import create_publisher
from faststream.sqs.subscriber.factory import create_subscriber

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.parser import CodecProto
    from faststream._internal.types import BrokerMiddleware, CustomCallable
    from faststream.sqs.publisher.usecase import SQSPublisher
    from faststream.sqs.schemas import SQSQueue
    from faststream.sqs.subscriber.usecase import SQSSubscriber


class SQSRegistrator(Registrator[SQSRawMessage, SQSBrokerConfig]):
    """Includable to SQSBroker router."""

    @override
    def subscriber(  # type: ignore[override]
        self,
        queue: Union[str, "SQSQueue"],
        *,
        wait_time_seconds: int = 5,
        max_messages: int = 10,
        visibility_timeout: int | None = None,
        batch: bool = False,
        # broker arguments
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        codec: Optional["CodecProto"] = None,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> "SQSSubscriber":
        """Subscribe a handler to an SQS queue.

        Args:
            queue: Queue name or an ``SQSQueue``/``FifoQueue`` declaration.
            wait_time_seconds: Long-poll wait time (0-20).
            max_messages: Max messages per receive (1-10).
            visibility_timeout: Per-receive visibility timeout override.
            batch: Consume up to ``max_messages`` at once; the handler receives a list.
            ack_policy: Acknowledgement policy for message processing.
            no_reply: Whether to disable FastStream RPC / reply-to responses.
            dependencies: Dependencies list to apply to the subscriber.
            parser: Custom parser to map raw messages to FastStream ones.
            decoder: Function to decode FastStream message bytes to Python objects.
            codec: Custom codec object.
            persistent: Whether to retain the subscriber across broker restarts.
            title: AsyncAPI subscriber object title.
            description: AsyncAPI subscriber object description.
            include_in_schema: Whether to include operation in AsyncAPI schema.
        """
        subscriber = create_subscriber(
            queue=queue,
            wait_time_seconds=wait_time_seconds,
            max_messages=max_messages,
            visibility_timeout=visibility_timeout,
            batch=batch,
            ack_policy=ack_policy,
            no_reply=no_reply,
            config=cast("SQSBrokerConfig", self.config),
            title_=title,
            description_=description,
            include_in_schema=include_in_schema,
        )

        super().subscriber(subscriber, persistent=persistent)

        return subscriber.add_call(
            parser_=parser or self._parser,
            decoder_=decoder or self._decoder,
            codec_=codec,
            dependencies_=dependencies,
        )

    @override
    def publisher(  # type: ignore[override]
        self,
        queue: Union[str, "SQSQueue"],
        *,
        headers: dict[str, str] | None = None,
        group_id: str | None = None,
        deduplication_id: str | None = None,
        delay_seconds: int = 0,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> "SQSPublisher":
        """Create a persistent publisher object for the given SQS queue.

        Args:
            queue: Queue name or an ``SQSQueue``/``FifoQueue`` declaration.
            headers: Default headers to include in every published message.
            group_id: Default ``MessageGroupId`` for FIFO queues.
            deduplication_id: Default ``MessageDeduplicationId`` for FIFO queues.
            delay_seconds: Default ``DelaySeconds`` for published messages.
            persistent: Whether to retain the publisher across broker restarts.
            title: AsyncAPI publisher object title.
            description: AsyncAPI publisher object description.
            schema: AsyncAPI publishing message type.
            include_in_schema: Whether to include operation in AsyncAPI schema.
        """
        publisher = create_publisher(
            queue=queue,
            headers=headers,
            group_id=group_id,
            deduplication_id=deduplication_id,
            delay_seconds=delay_seconds,
            config=cast("SQSBrokerConfig", self.config),
            title_=title,
            description_=description,
            schema_=schema,
            include_in_schema=include_in_schema,
        )
        super().publisher(publisher, persistent=persistent)
        return publisher

    @override
    def include_router(
        self,
        router: "SQSRegistrator",  # type: ignore[override]
        *,
        prefix: str = "",
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        include_in_schema: bool | None = None,
    ) -> None:
        if not isinstance(router, SQSRegistrator):
            msg = (
                f"Router must be an instance of SQSRegistrator, "
                f"got {type(router).__name__} instead."
            )
            raise SetupError(msg)

        super().include_router(
            router,
            prefix=prefix,
            dependencies=dependencies,
            middlewares=middlewares,
            include_in_schema=include_in_schema,
        )
