"""GCP Pub/Sub message producer."""

import asyncio
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from gcloud.aio.pubsub import PubsubMessage, PublisherClient

from faststream._internal.producer import ProducerProto
from faststream.message import gen_cor_id

if TYPE_CHECKING:
    from aiohttp import ClientSession


class GCPPubSubFastProducer(ProducerProto):
    """GCP Pub/Sub message producer."""
    
    def __init__(
        self,
        project_id: str,
        service_file: Optional[str] = None,
        emulator_host: Optional[str] = None,
    ) -> None:
        """Initialize producer.
        
        Args:
            project_id: GCP project ID
            service_file: Path to service account JSON file
            emulator_host: Pub/Sub emulator host
        """
        self.project_id = project_id
        self.service_file = service_file
        self.emulator_host = emulator_host
        self._publisher: Optional[PublisherClient] = None
        self._session: Optional["ClientSession"] = None
    
    async def publish(
        self,
        topic: str,
        data: bytes,
        attributes: Optional[Dict[str, str]] = None,
        ordering_key: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> str:
        """Publish a message to a topic.
        
        Args:
            topic: Topic name
            data: Message data
            attributes: Message attributes
            ordering_key: Message ordering key
            correlation_id: Correlation ID
        
        Returns:
            Published message ID
        """
        if not self._publisher:
            raise RuntimeError("Producer not initialized. Call connect() first.")
        
        # Add correlation ID to attributes if provided
        attrs = attributes or {}
        if correlation_id:
            attrs["correlation_id"] = correlation_id
        else:
            attrs["correlation_id"] = gen_cor_id()
        
        # Create message
        message = PubsubMessage(data, **attrs)
        if ordering_key:
            message.ordering_key = ordering_key
        
        # Format topic path
        topic_path = self._publisher.topic_path(self.project_id, topic)
        
        # Publish message
        result = await self._publisher.publish(topic_path, [message])
        
        # Return first message ID
        message_ids = result.get("messageIds", [])
        return message_ids[0] if message_ids else ""
    
    async def publish_batch(
        self,
        topic: str,
        messages: List[PubsubMessage],
    ) -> List[str]:
        """Publish multiple messages to a topic.
        
        Args:
            topic: Topic name
            messages: List of messages to publish
        
        Returns:
            List of published message IDs
        """
        if not self._publisher:
            raise RuntimeError("Producer not initialized. Call connect() first.")
        
        # Format topic path
        topic_path = self._publisher.topic_path(self.project_id, topic)
        
        # Add correlation IDs to messages
        for msg in messages:
            if "correlation_id" not in msg.attributes:
                msg.attributes["correlation_id"] = gen_cor_id()
        
        # Publish messages
        result = await self._publisher.publish(topic_path, messages)
        
        # Return message IDs
        return result.get("messageIds", [])
    
    async def request(
        self,
        topic: str,
        data: bytes,
        attributes: Optional[Dict[str, str]] = None,
        timeout: float = 30.0,
    ) -> Any:
        """Send a request and wait for response (not directly supported in Pub/Sub).
        
        Args:
            topic: Topic name
            data: Message data
            attributes: Message attributes
            timeout: Request timeout
        
        Returns:
            Response data
        
        Raises:
            NotImplementedError: Request-reply pattern requires custom implementation
        """
        raise NotImplementedError(
            "Request-reply pattern is not natively supported in GCP Pub/Sub. "
            "Consider implementing using correlation IDs and a response subscription."
        )