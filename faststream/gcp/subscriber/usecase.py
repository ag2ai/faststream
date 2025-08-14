"""GCP Pub/Sub subscriber use case."""

import contextlib
import logging
from collections.abc import Sequence
from contextlib import AsyncExitStack
from itertools import chain
from typing import TYPE_CHECKING, Any

import anyio
import backoff
from gcloud.aio.pubsub import PubsubMessage, SubscriberClient
from typing_extensions import override

from faststream._internal.endpoint.subscriber.mixins import TasksMixin
from faststream._internal.endpoint.subscriber.usecase import SubscriberUsecase
from faststream._internal.endpoint.utils import process_msg
from faststream.exceptions import SubscriberNotFound
from faststream.gcp.message import GCPMessage
from faststream.gcp.publisher.fake import GCPFakePublisher
from faststream.gcp.response_utils import ensure_gcp_response

if TYPE_CHECKING:
    from faststream._internal.endpoint.publisher import PublisherProto
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream._internal.middlewares import BaseMiddleware
    from faststream.gcp.configs.broker import GCPBrokerConfig
    from faststream.gcp.subscriber.config import GCPSubscriberConfig
    from faststream.gcp.subscriber.specification import (
        GCPSubscriberSpecification,
    )
    from faststream.message import StreamMessage as BrokerStreamMessage
    from faststream.response.response import Response


class GCPSubscriber(TasksMixin, SubscriberUsecase[PubsubMessage]):
    """GCP Pub/Sub subscriber implementation."""

    _outer_config: "GCPBrokerConfig"

    def __init__(
        self,
        config: "GCPSubscriberConfig",
        specification: "GCPSubscriberSpecification",
        calls: "CallsCollection[Any]",
    ) -> None:
        """Initialize subscriber.

        Args:
            config: Subscriber configuration
            specification: Subscriber specification
            calls: Handler calls collection
        """
        # Parser and decoder are already set in config.__post_init__

        super().__init__(config, specification, calls)
        self.config = config
        self.subscription = config.subscription
        self.topic = config.topic
        self.max_messages = config.max_messages

    def get_subscription_name(self) -> str:
        """Get subscription name with prefix applied."""
        return f"{self._outer_config.prefix}{self.subscription}"

    def get_topic_name(self) -> str | None:
        """Get topic name with prefix applied."""
        if self.topic:
            return f"{self._outer_config.prefix}{self.topic}"
        return None

    @property
    def _subscriber_client(self) -> SubscriberClient:
        """Get the subscriber client from broker."""
        # The connection state should have the subscriber client
        if (
            hasattr(self._outer_config, "connection")
            and self._outer_config.connection
            and self._outer_config.connection.subscriber
        ):
            return self._outer_config.connection.subscriber
        msg = "Subscriber client not available. Ensure broker is connected."
        raise RuntimeError(msg)

    def _make_response_publisher(
        self,
        message: "BrokerStreamMessage[PubsubMessage]",
    ) -> Sequence["PublisherProto"]:
        # GCP Pub/Sub requires a valid topic name - if reply_to is empty,
        # we can't create a publisher (unlike RabbitMQ where empty routing key is valid)
        if not message.reply_to:
            return ()

        return (
            GCPFakePublisher(
                self._outer_config.producer,
                topic=message.reply_to,
            ),
        )

    @override
    async def process_message(self, msg: PubsubMessage) -> "Response":
        """Execute all message processing stages with GCP-specific response handling."""
        context = self._outer_config.fd_config.context
        logger_state = self._outer_config.logger

        async with AsyncExitStack() as stack:
            stack.enter_context(self.lock)

            # Enter context before middlewares
            stack.enter_context(context.scope("logger", logger_state.logger.logger))
            for k, v in self._outer_config.extra_context.items():
                stack.enter_context(context.scope(k, v))

            # enter all middlewares
            middlewares: list[BaseMiddleware] = []
            for base_m in (
                self._SubscriberUsecase__build__middlewares_stack()  # type: ignore[attr-defined]
            ):  # Access private method
                middleware = base_m(msg, context=context)
                middlewares.append(middleware)
                await middleware.__aenter__()

            cache: dict[Any, Any] = {}
            parsing_error: Exception | None = None

            for h in self.calls:
                try:
                    message = await h.is_suitable(msg, cache)
                except Exception as e:
                    parsing_error = e
                    break

                if message is not None:
                    stack.enter_context(
                        context.scope("log_context", self.get_log_context(message)),
                    )
                    stack.enter_context(context.scope("message", message))

                    # Middlewares should be exited before scope release
                    for m in middlewares:
                        stack.push_async_exit(m.__aexit__)

                    # Use GCP-specific response handler for tuple support
                    result_msg = ensure_gcp_response(
                        await h.call(
                            message=message,
                            # consumer middlewares
                            _extra_middlewares=(
                                m.consume_scope for m in middlewares[::-1]
                            ),
                        ),
                    )

                    if not result_msg.correlation_id:
                        result_msg.correlation_id = message.correlation_id

                    # Publish to response publisher and handler publishers
                    for p in chain(
                        self._SubscriberUsecase__get_response_publisher(  # type: ignore[attr-defined]
                            message
                        ),  # Access private method
                        h.handler._publishers,
                    ):
                        await p._publish(
                            result_msg.as_publish_command(),
                            _extra_middlewares=(
                                m.publish_scope for m in middlewares[::-1]
                            ),
                        )

                    # Return data for tests
                    return result_msg

            # Suitable handler wasn't found or an error during msg validation
            if parsing_error:
                raise parsing_error

            error_msg = f"There is no suitable handler for {msg=}"
            raise SubscriberNotFound(error_msg)

        # An error was raised and processed by some middleware
        return ensure_gcp_response(None)

    @override
    async def start(
        self,
        *args: Any,
    ) -> None:
        if self.tasks:
            return

        await super().start()

        if self.config.create_subscription and self.topic:
            await self._ensure_subscription_exists()

        # Call _post_start to properly set running state BEFORE starting tasks
        self._post_start()

        start_signal = anyio.Event()

        if self.calls:
            self.add_task(self._consume(*args, start_signal=start_signal))

            with anyio.fail_after(3.0):
                await start_signal.wait()

        else:
            start_signal.set()

    async def _consume(self, *args: Any, start_signal: anyio.Event) -> None:
        """Main consume loop following FastStream pattern."""
        connected = True

        # Signal that we're ready immediately
        start_signal.set()

        self._log(
            log_level=logging.INFO,
            message=f"Starting consume loop for {self.get_subscription_name()}",
        )

        while self.running:
            try:
                await self._get_msgs(*args)

            except Exception as e:  # noqa: PERF203
                self._log(
                    log_level=logging.ERROR,
                    message=f"Message fetch error: {e}",
                    exc_info=e,
                )

                if connected:
                    connected = False

                await anyio.sleep(5.0)

            else:
                if not connected:
                    connected = True
                    self._log(
                        log_level=logging.INFO,
                        message=f"{self.get_subscription_name()} subscription connection established",
                    )

    async def _get_msgs(self, *args: Any) -> None:
        """Get messages from subscription."""
        try:
            subscriber = self._subscriber_client
            subscription_path = subscriber.subscription_path(
                self._outer_config.project_id, self.get_subscription_name()
            )

            # Pull messages with retry logic
            messages = await self._pull_with_retry(subscriber, subscription_path)

            if not messages:
                await anyio.sleep(0.1)  # Brief pause if no messages
                return

            self._log(
                log_level=logging.INFO,
                message=f"Pulled {len(messages)} messages from {self.get_subscription_name()}",
            )

            # Process messages concurrently
            for msg_data in messages:
                await self._process_message(msg_data, subscriber)
        except Exception as e:
            self._log(
                log_level=logging.ERROR,
                message=f"Error in _get_msgs: {e}",
                exc_info=e,
            )
            raise

    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=3,
        base=1.0,
        max_value=10.0,
    )
    async def _pull_with_retry(
        self, subscriber: SubscriberClient, subscription_path: str
    ) -> list[Any]:
        """Pull messages with exponential backoff retry."""
        # gcloud-aio returns a list of SubscriberMessage objects directly
        messages = await subscriber.pull(
            subscription_path, max_messages=self.max_messages
        )
        return messages or []

    async def _process_message(self, msg_data: Any, subscriber: SubscriberClient) -> None:
        """Process a single message using FastStream pattern."""
        try:
            # msg_data is a SubscriberMessage from gcloud-aio
            ack_id = msg_data.ack_id

            # Create PubsubMessage from SubscriberMessage
            # Use keyword arguments to preserve attribute structure
            attrs = msg_data.attributes or {}
            pubsub_msg = PubsubMessage(
                data=msg_data.data,
                message_id=msg_data.message_id,
                publish_time=msg_data.publish_time,
                **attrs,
            )

            # Use the inherited consume method which handles dependency injection
            # Pass the raw PubsubMessage as the base class expects MsgType
            await self.consume(pubsub_msg)

            # ACK the message after successful processing
            await self._ack_message(subscriber, ack_id)

        except Exception as e:
            # Handle processing error
            self._log(
                log_level=logging.ERROR,
                message=f"Error processing message: {e}",
                exc_info=e,
            )

            # NACK the message (modify ack deadline to 0)
            if "ack_id" in locals():
                await self._nack_message(subscriber, ack_id)

    def _parse_message(self, msg: PubsubMessage) -> GCPMessage:
        """Parse raw PubsubMessage into GCPMessage."""
        return GCPMessage(
            raw_message=msg,
            ack_id=None,  # Will be set by _process_message
            subscription=self.get_subscription_name(),
        )

    async def _ack_message(self, subscriber: SubscriberClient, ack_id: str) -> None:
        """Acknowledge a message."""
        subscription_path = subscriber.subscription_path(
            self._outer_config.project_id, self.get_subscription_name()
        )
        await subscriber.acknowledge(subscription_path, [ack_id])

    async def _nack_message(self, subscriber: SubscriberClient, ack_id: str) -> None:
        """Negative acknowledge a message."""
        subscription_path = subscriber.subscription_path(
            self._outer_config.project_id, self.get_subscription_name()
        )
        # Modify ack deadline to 0 to immediately make message available for redelivery
        await subscriber.modify_ack_deadline(subscription_path, [ack_id], 0)

    async def _ensure_subscription_exists(self) -> None:
        """Ensure the subscription exists."""
        try:
            # First ensure the topic exists (needed for subscription creation)
            topic_name = self.get_topic_name()
            if (
                topic_name
                and hasattr(self._outer_config, "connection")
                and self._outer_config.connection
            ):
                publisher_client = self._outer_config.connection.publisher
                assert publisher_client is not None
                topic_path = (
                    f"projects/{self._outer_config.project_id}/topics/{topic_name}"
                )
                # Topic might already exist
                with contextlib.suppress(Exception):  # nosec B110
                    await publisher_client.create_topic(topic_path)

            # Now create the subscription
            subscriber = self._subscriber_client
            subscription_path = f"projects/{self._outer_config.project_id}/subscriptions/{self.get_subscription_name()}"
            topic_path = f"projects/{self._outer_config.project_id}/topics/{self.get_topic_name() or ''}"

            await subscriber.create_subscription(
                subscription=subscription_path,
                topic=topic_path,
                body={"ackDeadlineSeconds": self.config.ack_deadline}
                if self.config.ack_deadline
                else None,
            )
        except Exception:  # nosec B110
            # Subscription might already exist or creation failed
            # In a production system, you'd want proper error handling and logging
            pass

    async def __aiter__(self) -> Any:
        """Async iterator for message consumption."""
        assert not self.calls, (
            "You can't use iterator method if subscriber has registered handlers."
        )

        subscriber = self._subscriber_client
        subscription_path = subscriber.subscription_path(
            self._outer_config.project_id, self.get_subscription_name()
        )
        context = self._outer_config.fd_config.context

        while self.running:
            # Pull messages from subscription
            messages = await subscriber.pull(subscription_path, max_messages=1)

            if not messages:
                # No messages available, short sleep and continue
                await anyio.sleep(0.1)
                continue

            # Process the first message
            msg_data = messages[0]

            # Convert to PubsubMessage if needed
            if hasattr(msg_data, "message"):
                raw_msg = msg_data.message
            else:
                # Create PubsubMessage from SubscriberMessage data
                raw_msg = PubsubMessage(
                    data=getattr(msg_data, "data", b""),
                    message_id=getattr(msg_data, "message_id", ""),
                    publish_time=getattr(msg_data, "publish_time", None),
                    attributes=getattr(msg_data, "attributes", {}),
                )

            # Use process_msg to properly set up parser and decoder like get_one() does
            msg = await process_msg(
                msg=raw_msg,
                middlewares=(
                    m(raw_msg, context=context) for m in self._broker_middlewares
                ),
                parser=self.config.parser,
                decoder=self.config.decoder,
            )

            if msg and isinstance(msg, GCPMessage):
                msg._ack_id = getattr(msg_data, "ack_id", None)
                msg._subscription = self.get_subscription_name()
                yield msg

    async def get_one(self, *, timeout: float = 5.0) -> "BrokerStreamMessage[Any] | None":
        """Get a single message from the subscription."""
        assert not self.calls, (
            "You can't use `get_one` method if subscriber has registered handlers."
        )
        subscriber = self._subscriber_client
        subscription_path = subscriber.subscription_path(
            self._outer_config.project_id, self.get_subscription_name()
        )

        # Pull a single message with timeout using anyio.move_on_after
        messages = None
        with anyio.move_on_after(timeout):
            messages = await subscriber.pull(subscription_path, max_messages=1)

        if not messages:
            # Either no messages available or timeout exceeded, return None
            return None

        # Process the first message
        msg_data = messages[0]
        # Convert to PubsubMessage if needed
        if hasattr(msg_data, "message"):
            raw_msg = msg_data.message
        else:
            # Create PubsubMessage from SubscriberMessage data
            raw_msg = PubsubMessage(
                data=getattr(msg_data, "data", b""),
                message_id=getattr(msg_data, "message_id", ""),
                publish_time=getattr(msg_data, "publish_time", None),
                attributes=getattr(msg_data, "attributes", {}),
            )

        # Use process_msg to properly set up parser and decoder
        context = self._outer_config.fd_config.context

        msg = await process_msg(
            msg=raw_msg,
            middlewares=(m(raw_msg, context=context) for m in self._broker_middlewares),
            parser=self.config.parser,
            decoder=self.config.decoder,
        )

        # Set additional GCP Pub/Sub specific attributes
        if msg and isinstance(msg, GCPMessage):
            msg._ack_id = getattr(msg_data, "ack_id", None)
            msg._subscription = self.get_subscription_name()

        return msg
