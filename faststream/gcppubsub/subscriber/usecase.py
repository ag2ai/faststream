"""GCP Pub/Sub subscriber use case."""

import asyncio
from typing import TYPE_CHECKING, Any, Optional

import backoff
from gcloud.aio.pubsub import PubsubMessage, SubscriberClient

from faststream._internal.endpoint.subscriber.usecase import SubscriberUsecase
from faststream.gcppubsub.message import GCPPubSubMessage

if TYPE_CHECKING:
    from faststream.gcppubsub.broker.registrator import GCPPubSubRegistrator


class GCPPubSubSubscriber(SubscriberUsecase[PubsubMessage]):
    """GCP Pub/Sub subscriber implementation."""
    
    def __init__(
        self,
        subscription: str,
        *,
        broker: "GCPPubSubRegistrator",
        topic: Optional[str] = None,
        create_subscription: bool = True,
        ack_deadline: Optional[int] = None,
        max_messages: int = 10,
        **kwargs: Any,
    ) -> None:
        """Initialize subscriber.
        
        Args:
            subscription: Subscription name
            broker: Broker instance
            topic: Topic name (required if creating subscription)
            create_subscription: Whether to create subscription if it doesn't exist
            ack_deadline: ACK deadline in seconds
            max_messages: Maximum messages to pull at once
            **kwargs: Additional options
        """
        self.subscription = subscription
        self.broker = broker
        self.topic = topic
        self.create_subscription = create_subscription
        self.ack_deadline = ack_deadline or broker.config.subscriber_ack_deadline
        self.max_messages = max_messages
        self._running = False
        self._task: Optional[asyncio.Task] = None
        
        super().__init__(**kwargs)
    
    async def start(self) -> None:
        """Start the subscriber."""
        if self.create_subscription and self.topic:
            await self._ensure_subscription_exists()
        
        self._running = True
        self._task = asyncio.create_task(self._pull_messages())
    
    async def stop(self) -> None:
        """Stop the subscriber."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
    
    async def _pull_messages(self) -> None:
        """Continuously pull messages from subscription."""
        subscriber = self._get_subscriber_client()
        subscription_path = subscriber.subscription_path(
            self.broker.project_id, 
            self.subscription
        )
        
        while self._running:
            try:
                # Pull messages with retry logic
                messages = await self._pull_with_retry(subscriber, subscription_path)
                
                if not messages:
                    await asyncio.sleep(0.1)  # Brief pause if no messages
                    continue
                
                # Process messages concurrently
                tasks = []
                for msg_data in messages:
                    task = asyncio.create_task(self._process_message(msg_data, subscriber))
                    tasks.append(task)
                
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                # Log error and continue
                if hasattr(self.broker, 'logger'):
                    self.broker.logger.error(f"Error in message pull loop: {e}")
                await asyncio.sleep(1.0)  # Pause before retrying
    
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
        response = await subscriber.pull(
            subscription_path, 
            max_messages=self.max_messages
        )
        return response.get('receivedMessages', [])
    
    async def _process_message(self, msg_data: dict, subscriber: SubscriberClient) -> None:
        """Process a single message."""
        try:
            # Extract message and ack_id
            raw_message = msg_data.get('message', {})
            ack_id = msg_data.get('ackId')
            
            # Create PubsubMessage
            pubsub_msg = PubsubMessage(
                data=raw_message.get('data', b''),
                message_id=raw_message.get('messageId'),
                publish_time=raw_message.get('publishTime'),
                attributes=raw_message.get('attributes', {}),
            )
            
            # Wrap in FastStream message
            fs_message = GCPPubSubMessage(
                raw_message=pubsub_msg,
                ack_id=ack_id,
                subscription=self.subscription,
            )
            
            # Process message through handler
            await self._handle_message(fs_message)
            
            # Acknowledge message if processing succeeded
            if not fs_message.acknowledged:
                await self._ack_message(subscriber, ack_id)
                
        except Exception as e:
            # Handle processing error
            if hasattr(self.broker, 'logger'):
                self.broker.logger.error(f"Error processing message: {e}")
            
            # Nack the message (modify ack deadline to 0)
            if 'ack_id' in locals():
                await self._nack_message(subscriber, ack_id)
    
    async def _handle_message(self, message: GCPPubSubMessage) -> None:
        """Handle a message through the registered handler."""
        if self.handler:
            try:
                await self.handler(message)
                message._acknowledged = True
            except Exception as e:
                message._acknowledged = False
                raise
    
    async def _ack_message(self, subscriber: SubscriberClient, ack_id: str) -> None:
        """Acknowledge a message."""
        subscription_path = subscriber.subscription_path(
            self.broker.project_id, 
            self.subscription
        )
        await subscriber.acknowledge(subscription_path, [ack_id])
    
    async def _nack_message(self, subscriber: SubscriberClient, ack_id: str) -> None:
        """Negative acknowledge a message."""
        subscription_path = subscriber.subscription_path(
            self.broker.project_id, 
            self.subscription
        )
        # Modify ack deadline to 0 to immediately make message available for redelivery
        await subscriber.modify_ack_deadline(subscription_path, [ack_id], 0)
    
    def _get_subscriber_client(self) -> SubscriberClient:
        """Get the subscriber client from broker."""
        if hasattr(self.broker, '_state') and self.broker._state.subscriber:
            return self.broker._state.subscriber
        raise RuntimeError("Subscriber client not available. Ensure broker is connected.")
    
    async def _ensure_subscription_exists(self) -> None:
        """Ensure the subscription exists."""
        try:
            subscriber = self._get_subscriber_client()
            # Note: gcloud-aio has a create_subscription method
            await subscriber.create_subscription(
                subscription_name=self.subscription,
                topic_name=self.topic,
                ack_deadline=self.ack_deadline,
            )
        except Exception:
            # Subscription might already exist or creation failed
            # In a production system, you'd want proper error handling
            pass