from abc import abstractmethod
from collections.abc import Iterable
from typing import (
    TYPE_CHECKING,
    Any,
    NamedTuple,
    Optional,
)

from nats.js.api import ConsumerConfig
from typing_extensions import override

from faststream._internal.endpoint.derived import Resolved
from faststream._internal.endpoint.subscriber.usecase import SubscriberUsecase
from faststream._internal.types import MsgType
from faststream._internal.utils.path import Address
from faststream.nats.publisher.fake import NatsFakePublisher
from faststream.nats.schemas import JStream
from faststream.nats.schemas.js_stream import NATS_ADDRESS_SYNTAX
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


class _ResolvedOptions(NamedTuple):
    """What a NATS Subscriber listens on, once its composition is final.

    Resolved together, so that the reads below cannot disagree with each other
    or with what the subscription was created against. NATS has the widest
    surface of the six — a subject, a queue group, a durable name, a stream and
    a list of filter subjects — and every one of them can arrive from a Config
    value or wear the Router prefix.

    `consumer_config` is the exception, held rather than built: it is the
    Subscriber's own options object, which Preparation writes the resolved
    durable name into. What it earns here is the refusal, not a copy — an early
    read of it is an early read of the durable name.
    """

    subject: Address
    queue: str
    durable: str | None
    stream: JStream | None
    filters: list[Address]
    consumer_config: ConsumerConfig


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
        self._queue = config.queue
        self._durable = config.durable
        self._stream = config.stream
        self._sub_config = config.sub_config

        self._extra_options = config.extra_options or {}

        # What the consumer options were declared with, so that a durable name
        # filled in from a Config value can be filled in again, against the
        # values of the next connection.
        self._declared_durable_name = config.sub_config.durable_name

        self._resolved: Resolved[_ResolvedOptions] = self._derived.add(
            # "options" rather than "addresses": a queue group, a durable name
            # and the consumer options resolve here too, and the refusal names
            # what the caller actually asked for.
            Resolved("a Subscriber's options"),
        )

        self._fetch_sub = None
        self.subscription = None

    @override
    def _prepare(self) -> None:
        """Resolve what this Subscriber listens on, before anything reads it.

        First, because everything performed afterwards — the address check, the
        parser holding a capture regex, the log context the logger is built from
        — reads these values back as fields.
        """
        config = self._outer_config
        declared = self._subject

        durable = config.resolve_option(self._durable)

        # The registrar used to fill the durable name in, but a `durable`
        # placeholder has nothing to resolve against there. It is the same write
        # into the same options object, only later — and driven off what was
        # *declared* rather than off what is in the object, so that a name filled
        # in for one connection is not read back as a declaration by the next.
        if self._declared_durable_name is None:
            self._sub_config.durable_name = durable

        self._resolved.set(
            _ResolvedOptions(
                subject=Address(
                    config.resolve_address(declared),
                    NATS_ADDRESS_SYNTAX,
                    config.config_key(declared),
                ),
                # `resolve_option` rather than `resolve_address`: a queue group
                # names a set of consumers rather than a place on the server, and
                # a literal one has never carried the Router prefix.
                queue=config.resolve_option(self._queue),
                durable=durable,
                # A Config value may be a stream name or a whole prepared
                # `JStream`; either way the object is built after resolution,
                # which is what lets one arrive from configuration at all.
                stream=JStream.validate(config.resolve_option(self._stream)),
                filters=[
                    Address(subject, NATS_ADDRESS_SYNTAX).add_prefix(config.prefix)
                    for subject in (self._sub_config.filter_subjects or ())
                ],
                consumer_config=self._sub_config,
            ),
        )

        super()._prepare()

    @property
    def subject(self) -> "Address":
        """The subject this Subscriber listens on, and its Broker address."""
        return self._resolved.get().subject

    @property
    def queue(self) -> str:
        """The queue group this Subscriber joins, empty when it joins none."""
        return self._resolved.get().queue

    @property
    def durable(self) -> str | None:
        """The name of the server-side consumer this Subscriber binds to."""
        return self._resolved.get().durable

    @property
    def stream(self) -> JStream | None:
        """The stream this Subscriber consumes from."""
        return self._resolved.get().stream

    @property
    def config(self) -> "ConsumerConfig":
        """The JetStream consumer options, with the durable name filled into them.

        Read back through Preparation rather than straight off the options
        object, so that asking before the durable name was filled in refuses
        instead of answering with the options as they were declared.
        """
        return self._resolved.get().consumer_config

    @property
    def extra_options(self) -> dict[str, Any]:
        """The subscription arguments, with the addresses among them resolved."""
        if (stream := self.stream) is None:
            return self._extra_options

        return self._extra_options | {"durable": self.durable, "stream": stream.name}

    @property
    def filter_addresses(self) -> list["Address"]:
        """The subjects a JetStream consumer filters on, each read as an Address."""
        return self._resolved.get().filters

    @property
    def filter_subjects(self) -> list[str]:
        return [address.broker_address for address in self.filter_addresses]

    @override
    def subscription_addresses(self) -> Iterable["Address"]:
        if subject := self.subject:
            # A declared subject is the address `Path()` is read from; filter
            # subjects only narrow which of its messages the consumer is handed,
            # and the parser never matches against them. Checking them here would
            # reject working subscribers — see ADR-0004, which carries this as the
            # one narrowing of "the group must be present in all of them".
            yield subject
            return

        yield from self.filter_addresses

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
        return self.subject.template or ", ".join(self.filter_subjects or ())


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
            subject=self.subject.template,
        )
