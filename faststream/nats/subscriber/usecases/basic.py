from abc import abstractmethod
from collections.abc import Iterable
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
)

from faststream._internal.endpoint.subscriber.usecase import SubscriberUsecase
from faststream._internal.types import MsgType
from faststream.nats.publisher.fake import NatsFakePublisher
from faststream.nats.schemas.js_stream import compile_nats_wildcard
from faststream.nats.subscriber.adapters import (
    Unsubscriptable,
)

if TYPE_CHECKING:
    from nats.aio.client import Client
    from nats.js import JetStreamContext

    from faststream._internal.endpoint.publisher import PublisherProto
    from faststream._internal.endpoint.subscriber import SubscriberSpecification
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream.message import StreamMessage
    from faststream.nats.configs import NatsBrokerConfig
    from faststream.nats.subscriber.config import NatsSubscriberConfig


class LogicSubscriber(SubscriberUsecase[MsgType]):
    """Basic class for all NATS Subscriber types (KeyValue, ObjectStorage, Core & JetStream)."""

    subscription: Unsubscriptable | None
    _fetch_sub: Unsubscriptable | None
    _outer_config: "NatsBrokerConfig"

    def __init__(
        self,
        config: "NatsSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[MsgType]",
    ) -> None:
        super().__init__(config, specification, calls)

        self._subject = config.subject
        self.config = config.sub_config

        self.extra_options = config.extra_options or {}

        self._fetch_sub = None
        self.subscription = None

    @property
    def subject(self) -> str:
        return f"{self._outer_config.prefix}{self._subject}"

    @property
    def filter_subjects(self) -> list[str]:
        prefix = self._outer_config.prefix
        return [f"{prefix}{subject}" for subject in (self.config.filter_subjects or ())]

    @property
    def clear_subject(self) -> str:
        """Compile `test.{name}` to `test.*` subject."""
        _, path = compile_nats_wildcard(self.subject)
        return path

    @property
    def connection(self) -> "Client":
        return self._outer_config.connection_state.connection

    @property
    def jetstream(self) -> "JetStreamContext":
        return self._outer_config.connection_state.stream

    async def start(self) -> None:
        """Create NATS subscription and start consume tasks."""
        await super().start()

        if self.calls:
            await self._create_subscription()

        self._post_start()

    async def stop(self) -> None:
        """Clean up handler subscription, cancel consume task in graceful mode."""
        await super().stop()

        if self.subscription is not None:
            await self.subscription.unsubscribe()
            self.subscription = None

        if self._fetch_sub is not None:
            await self._fetch_sub.unsubscribe()
            self._fetch_sub = None

    @abstractmethod
    async def _create_subscription(self) -> None:
        """Create NATS subscription object to consume messages."""
        raise NotImplementedError

    @staticmethod
    def build_log_context(
        message: Optional["StreamMessage[MsgType]"],
        subject: str,
        *,
        queue: str = "",
        stream: str = "",
    ) -> dict[str, str]:
        """Static method to build log context out of `self.consume` scope."""
        return {
            "subject": subject,
            "queue": queue,
            "stream": stream,
            "message_id": getattr(message, "message_id", ""),
        }

    @property
    def _resolved_subject_string(self) -> str:
        return self.subject or ", ".join(self.filter_subjects or ())


class DefaultSubscriber(LogicSubscriber[MsgType]):
    """Basic class for Core & JetStream Subscribers."""

    def _make_response_publisher(
        self,
        message: "StreamMessage[Any]",
    ) -> Iterable["PublisherProto"]:
        """Create Publisher objects to use it as one of `publishers` in `self.consume` scope."""
        return (
            NatsFakePublisher(
                producer=self._outer_config.producer,
                subject=message.reply_to,
            ),
        )

    def get_log_context(
        self,
        message: Optional["StreamMessage[MsgType]"],
    ) -> dict[str, str]:
        """Log context factory using in `self.consume` scope."""
        return self.build_log_context(
            message=message,
            subject=self.subject,
        )
