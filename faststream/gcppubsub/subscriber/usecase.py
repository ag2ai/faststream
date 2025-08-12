"""GCP Pub/Sub subscriber use case."""

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Optional, Sequence

import anyio
import backoff
from gcloud.aio.pubsub import PubsubMessage, SubscriberClient
from typing_extensions import override

from faststream._internal.endpoint.publisher import PublisherProto
from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream._internal.endpoint.subscriber.mixins import TasksMixin
from faststream._internal.endpoint.subscriber.usecase import SubscriberUsecase
from faststream._internal.endpoint.utils import process_msg
from faststream.gcppubsub.message import GCPPubSubMessage
from faststream.gcppubsub.parser import GCPPubSubParser
from faststream.gcppubsub.publisher.fake import GCPPubSubFakePublisher
from faststream.gcppubsub.subscriber.config import GCPPubSubSubscriberConfig
from faststream.gcppubsub.subscriber.specification import GCPPubSubSubscriberSpecification
from faststream.message import StreamMessage as BrokerStreamMessage

if TYPE_CHECKING:
    from faststream.gcppubsub.configs.broker import GCPPubSubBrokerConfig


class GCPPubSubSubscriber(TasksMixin, SubscriberUsecase[PubsubMessage]):
    """GCP Pub/Sub subscriber implementation."""
    
    _outer_config: "GCPPubSubBrokerConfig"

    def __init__(
        self,
        config: "GCPPubSubSubscriberConfig",
        specification: "GCPPubSubSubscriberSpecification",
        calls: "CallsCollection[Any]",
    ) -> None:
        """Initialize subscriber.
        
        Args:
            config: Subscriber configuration
            specification: Subscriber specification
            calls: Handler calls collection
        """
        # Create parser for message processing
        parser = GCPPubSubParser()
        config.decoder = parser.decode_message
        config.parser = parser.parse_message
        
        super().__init__(config, specification, calls)
        self.config = config
        self.subscription = config.subscription
        self.topic = config.topic
        self.max_messages = config.max_messages
    
    @property
    def _subscriber_client(self) -> SubscriberClient:
        """Get the subscriber client from broker."""
        # The connection state should have the subscriber client
        if hasattr(self._outer_config, 'connection') and self._outer_config.connection:
            if self._outer_config.connection.subscriber:
                return self._outer_config.connection.subscriber
        raise RuntimeError("Subscriber client not available. Ensure broker is connected.")
    
    def _make_response_publisher(
        self,
        message: "BrokerStreamMessage[PubsubMessage]",
    ) -> Sequence["PublisherProto"]:
        return (
            GCPPubSubFakePublisher(
                self._outer_config.producer,
                topic=message.reply_to,
            ),
        )

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
            message=f"Starting consume loop for {self.subscription}",
        )
        
        while self.running:
            try:
                await self._get_msgs(*args)
                
            except Exception as e:
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
                        message=f"{self.subscription} subscription connection established",
                    )
    
    async def _get_msgs(self, *args: Any) -> None:
        """Get messages from subscription."""
        try:
            subscriber = self._subscriber_client
            subscription_path = subscriber.subscription_path(
                self._outer_config.project_id, 
                self.subscription
            )
            
            # Pull messages with retry logic
            messages = await self._pull_with_retry(subscriber, subscription_path)
            
            if not messages:
                await anyio.sleep(0.1)  # Brief pause if no messages
                return
            
            self._log(
                log_level=logging.INFO,
                message=f"Pulled {len(messages)} messages from {self.subscription}",
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
        self, 
        subscriber: SubscriberClient, 
        subscription_path: str
    ) -> list:
        """Pull messages with exponential backoff retry."""
        # gcloud-aio returns a list of SubscriberMessage objects directly
        messages = await subscriber.pull(
            subscription_path, 
            max_messages=self.max_messages
        )
        return messages or []
    
    async def _process_message(self, msg_data: Any, subscriber: SubscriberClient) -> None:
        """Process a single message using FastStream pattern."""
        try:
            # msg_data is a SubscriberMessage from gcloud-aio
            ack_id = msg_data.ack_id
            
            # Create PubsubMessage from SubscriberMessage
            pubsub_msg = PubsubMessage(
                data=msg_data.data,
                message_id=msg_data.message_id,
                publish_time=msg_data.publish_time,
                attributes=msg_data.attributes or {},
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
            if 'ack_id' in locals():
                await self._nack_message(subscriber, ack_id)
    
    def _parse_message(self, msg: PubsubMessage) -> GCPPubSubMessage:
        """Parse raw PubsubMessage into GCPPubSubMessage."""
        return GCPPubSubMessage(
            raw_message=msg,
            ack_id=None,  # Will be set by _process_message
            subscription=self.subscription,
        )
    
    async def _ack_message(self, subscriber: SubscriberClient, ack_id: str) -> None:
        """Acknowledge a message."""
        subscription_path = subscriber.subscription_path(
            self._outer_config.project_id, 
            self.subscription
        )
        await subscriber.acknowledge(subscription_path, [ack_id])
    
    async def _nack_message(self, subscriber: SubscriberClient, ack_id: str) -> None:
        """Negative acknowledge a message."""
        subscription_path = subscriber.subscription_path(
            self._outer_config.project_id, 
            self.subscription
        )
        # Modify ack deadline to 0 to immediately make message available for redelivery
        await subscriber.modify_ack_deadline(subscription_path, [ack_id], 0)
    
    async def _ensure_subscription_exists(self) -> None:
        """Ensure the subscription exists."""
        try:
            subscriber = self._subscriber_client
            # Note: gcloud-aio has a create_subscription method
            await subscriber.create_subscription(
                subscription_name=self.subscription,
                topic_name=self.topic,
                ack_deadline=self.config.ack_deadline,
            )
        except Exception:
            # Subscription might already exist or creation failed
            # In a production system, you'd want proper error handling
            pass