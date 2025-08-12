"""GCP Pub/Sub message producer."""

import asyncio
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from gcloud.aio.pubsub import PubsubMessage, PublisherClient

from faststream._internal.producer import ProducerProto
from faststream.gcppubsub.response import GCPPubSubPublishCommand
from faststream.message import gen_cor_id

if TYPE_CHECKING:
    from aiohttp import ClientSession


class GCPPubSubFastProducer(ProducerProto[GCPPubSubPublishCommand]):
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
        cmd: GCPPubSubPublishCommand,
    ) -> str:
        """Publish a message to a topic.
        
        Args:
            cmd: Publish command with message, topic, and options
        
        Returns:
            Published message ID
        """
        if not self._publisher:
            raise RuntimeError("Producer not initialized. Call connect() first.")
        
        # Extract data from command
        message_data = cmd.message
        topic = cmd.topic
        attrs = cmd.attributes or {}
        ordering_key = cmd.ordering_key
        correlation_id = cmd.correlation_id or gen_cor_id()
        
        # Ensure correlation_id in attributes
        attrs["correlation_id"] = correlation_id
        
        # Convert message to bytes if needed
        if isinstance(message_data, str):
            data = message_data.encode()
        elif isinstance(message_data, bytes):
            data = message_data
        else:
            # Serialize other types to JSON then bytes
            import json
            data = json.dumps(message_data).encode()
        
        # Create message - gcloud-aio-pubsub expects data and keyword args for attributes
        message = PubsubMessage(data, ordering_key=ordering_key or "", **attrs)
        
        # Format topic path
        topic_path = self._publisher.topic_path(self.project_id, topic)
        
        # Publish message
        result = await self._publisher.publish(topic_path, [message])
        
        # Return first message ID
        message_ids = result.get("messageIds", [])
        return message_ids[0] if message_ids else ""
    
    async def publish_batch(
        self,
        cmd: GCPPubSubPublishCommand,
    ) -> List[str]:
        """Publish multiple messages to a topic.
        
        Args:
            cmd: Batch publish command
        
        Returns:
            List of published message IDs
        """
        if not self._publisher:
            raise RuntimeError("Producer not initialized. Call connect() first.")
        
        # For now, batch publishing isn't commonly used, so we'll implement a basic version
        # that just calls publish() for each message
        if hasattr(cmd, 'messages') and cmd.messages:
            message_ids = []
            for msg in cmd.messages:
                # Create a single message command
                single_cmd = type('Command', (), {
                    'message': msg,
                    'topic': cmd.topic,
                    'attributes': getattr(cmd, 'attributes', {}),
                    'ordering_key': getattr(cmd, 'ordering_key', None),
                    'correlation_id': gen_cor_id(),
                })
                msg_id = await self.publish(single_cmd)
                message_ids.append(msg_id)
            return message_ids
        return []
    
    async def request(
        self,
        cmd: GCPPubSubPublishCommand,
    ) -> Any:
        """Send a request and wait for response (not directly supported in Pub/Sub).
        
        Args:
            cmd: Request command with message, topic, and options
        
        Returns:
            Response data
        
        Raises:
            NotImplementedError: Request-reply pattern requires custom implementation
        """
        raise NotImplementedError(
            "Request-reply pattern is not natively supported in GCP Pub/Sub. "
            "Consider implementing using correlation IDs and a response subscription."
        )