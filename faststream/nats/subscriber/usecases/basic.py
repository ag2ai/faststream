from abc import abstractmethod
from collections.abc import Iterable
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
)

from typing_extensions import override

from faststream._internal.subscriber.usecase import SubscriberUsecase
from faststream._internal.types import MsgType
from faststream.nats.helpers import KVBucketDeclarer, OSBucketDeclarer
from faststream.nats.publisher.fake import NatsFakePublisher
from faststream.nats.schemas.js_stream import compile_nats_wildcard
from faststream.nats.subscriber.adapters import (
    Unsubscriptable,
)
from faststream.nats.subscriber.state import (
    ConnectedSubscriberState,
    EmptySubscriberState,
    SubscriberState,
)

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant
    from nats.js.api import ConsumerConfig

    from faststream._internal.basic_types import (
        AnyDict,
    )
    from faststream._internal.publisher.proto import BasePublisherProto, ProducerProto
    from faststream._internal.state import (
        BrokerState as BasicState,
        Pointer,
    )
    from faststream._internal.types import (
        AsyncCallable,
        BrokerMiddleware,
        CustomCallable,
    )
    from faststream.message import StreamMessage
    from faststream.middlewares import AckPolicy
    from faststream.nats.broker.state import BrokerState
    from faststream.nats.helpers import KVBucketDeclarer, OSBucketDeclarer


class LogicSubscriber(SubscriberUsecase[MsgType]):
    """Basic class for all NATS Subscriber types (KeyValue, ObjectStorage, Core & JetStream)."""

    subscription: Optional[Unsubscriptable]
    _fetch_sub: Optional[Unsubscriptable]
    producer: Optional["ProducerProto"]

    def __init__(
        self,
        *,
        subject: str,
        config: "ConsumerConfig",
        extra_options: Optional["AnyDict"],
        # Subscriber args
        default_parser: "AsyncCallable",
        default_decoder: "AsyncCallable",
        ack_policy: "AckPolicy",
        no_reply: bool,
        broker_dependencies: Iterable["Dependant"],
        broker_middlewares: Iterable["BrokerMiddleware[MsgType]"],
    ) -> None:
        self.subject = subject
        self.config = config

        self.extra_options = extra_options or {}

        super().__init__(
            default_parser=default_parser,
            default_decoder=default_decoder,
            # Propagated args
            ack_policy=ack_policy,
            no_reply=no_reply,
            broker_middlewares=broker_middlewares,
            broker_dependencies=broker_dependencies,
        )

        self._fetch_sub = None
        self.subscription = None
        self.producer = None

        self._connection_state: SubscriberState = EmptySubscriberState()

    @override
    def _setup(  # type: ignore[override]
        self,
        *,
        connection_state: "BrokerState",
        os_declarer: "OSBucketDeclarer",
        kv_declarer: "KVBucketDeclarer",
        # basic args
        extra_context: "AnyDict",
        # broker options
        broker_parser: Optional["CustomCallable"],
        broker_decoder: Optional["CustomCallable"],
        # dependant args
        state: "Pointer[BasicState]",
    ) -> None:
        self._connection_state = ConnectedSubscriberState(
            parent_state=connection_state,
            os_declarer=os_declarer,
            kv_declarer=kv_declarer,
        )

        super()._setup(
            extra_context=extra_context,
            broker_parser=broker_parser,
            broker_decoder=broker_decoder,
            state=state,
        )

    @property
    def clear_subject(self) -> str:
        """Compile `test.{name}` to `test.*` subject."""
        _, path = compile_nats_wildcard(self.subject)
        return path

    async def start(self) -> None:
        """Create NATS subscription and start consume tasks."""
        await super().start()

        if self.calls:
            await self._create_subscription()

    async def close(self) -> None:
        """Clean up handler subscription, cancel consume task in graceful mode."""
        await super().close()

        if self.subscription is not None:
            await self.subscription.unsubscribe()
            self.subscription = None

        if self._fetch_sub is not None:
            await self._fetch_sub.unsubscribe()
            self.subscription = None

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

    def add_prefix(self, prefix: str) -> None:
        """Include Subscriber in router."""
        if self.subject:
            self.subject = f"{prefix}{self.subject}"
        else:
            self.config.filter_subjects = [
                f"{prefix}{subject}" for subject in (self.config.filter_subjects or ())
            ]

    @property
    def _resolved_subject_string(self) -> str:
        return self.subject or ", ".join(self.config.filter_subjects or ())


class DefaultSubscriber(LogicSubscriber[MsgType]):
    """Basic class for Core & JetStream Subscribers."""

    def __init__(
        self,
        *,
        subject: str,
        config: "ConsumerConfig",
        # default args
        extra_options: Optional["AnyDict"],
        # Subscriber args
        default_parser: "AsyncCallable",
        default_decoder: "AsyncCallable",
        ack_policy: "AckPolicy",
        no_reply: bool,
        broker_dependencies: Iterable["Dependant"],
        broker_middlewares: Iterable["BrokerMiddleware[MsgType]"],
    ) -> None:
        super().__init__(
            subject=subject,
            config=config,
            extra_options=extra_options,
            # subscriber args
            default_parser=default_parser,
            default_decoder=default_decoder,
            # Propagated args
            ack_policy=ack_policy,
            no_reply=no_reply,
            broker_middlewares=broker_middlewares,
            broker_dependencies=broker_dependencies,
        )

    def _make_response_publisher(
        self,
        message: "StreamMessage[Any]",
    ) -> Iterable["BasePublisherProto"]:
        """Create Publisher objects to use it as one of `publishers` in `self.consume` scope."""
        return (
            NatsFakePublisher(
                producer=self._state.get().producer,
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
