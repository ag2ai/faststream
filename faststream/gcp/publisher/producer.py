"""GCP Pub/Sub message producer."""

import contextlib
from typing import TYPE_CHECKING, Any

from gcloud.aio.pubsub import PublisherClient, PubsubMessage

from faststream._internal.producer import ProducerProto
from faststream.gcp.response import GCPPublishCommand
from faststream.message import gen_cor_id

if TYPE_CHECKING:
    from aiohttp import ClientSession


class GCPFastProducer(ProducerProto[GCPPublishCommand]):
    """GCP Pub/Sub message producer."""

    def __init__(
        self,
        project_id: str,
        service_file: str | None = None,
        emulator_host: str | None = None,
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
        self._publisher: PublisherClient | None = None
        self._session: ClientSession | None = None

        # ProducerProto interface compliance
        # GCP Pub/Sub handles serialization internally, but we need these for interface compliance
        from faststream._internal.utils.functions import return_input

        self._parser: Any = return_input  # Pass-through function
        self._decoder: Any = return_input  # Pass-through function

    async def publish(
        self,
        cmd: GCPPublishCommand,
    ) -> str:
        """Publish a message to a topic.

        Args:
            cmd: Publish command with message, topic, and options

        Returns:
            Published message ID
        """
        if not self._publisher:
            msg = "Producer not initialized. Call connect() first."
            raise RuntimeError(msg)

        # Extract data from command
        message_data = cmd.message
        topic = cmd.topic
        attrs = cmd.attributes or {}
        ordering_key = cmd.ordering_key
        correlation_id = cmd.correlation_id or gen_cor_id()

        # Ensure correlation_id in attributes
        attrs["correlation_id"] = correlation_id

        # Convert message to bytes - handle FastStream encoding with custom serialization fallback
        from faststream.message import encode_message

        try:
            # Try FastStream's encoding first
            data, content_type = encode_message(message_data, serializer=None)

            # GCP Pub/Sub doesn't allow empty messages, use a single space as minimal payload
            if not data:
                data = b" "
                attrs["__faststream_empty"] = "true"  # Mark as originally empty

            # Add content type to attributes if available
            if content_type:
                attrs["content_type"] = content_type

        except TypeError as e:
            if "not JSON serializable" in str(e):
                # Handle non-serializable objects with custom encoder
                import json
                from dataclasses import asdict, is_dataclass
                from datetime import datetime

                def json_serializer(obj: Any) -> Any:
                    """JSON serializer for objects not serializable by default json code."""
                    if isinstance(obj, datetime):
                        return obj.isoformat()
                    if is_dataclass(obj) and not isinstance(obj, type):
                        return asdict(obj)
                    if hasattr(obj, "model_dump"):  # Pydantic v2
                        return obj.model_dump()
                    if hasattr(obj, "dict"):  # Pydantic v1
                        return obj.dict()
                    error_msg = f"Object of type {obj.__class__.__name__} is not JSON serializable"
                    raise TypeError(error_msg)

                data = json.dumps(message_data, default=json_serializer).encode()
                attrs["content_type"] = "application/json"
            else:
                raise

        # Create message - gcloud-aio-pubsub expects data and keyword args for attributes
        message = PubsubMessage(data, ordering_key=ordering_key or "", **attrs)

        # Format topic path
        topic_path = self._publisher.topic_path(self.project_id, topic)

        # Ensure topic exists (create if needed)
        await self._ensure_topic_exists(topic_path)

        # Publish message
        result = await self._publisher.publish(topic_path, [message])

        # Return first message ID
        message_ids = result.get("messageIds", [])
        return message_ids[0] if message_ids else ""

    async def publish_batch(
        self,
        cmd: GCPPublishCommand,
    ) -> list[str]:
        """Publish multiple messages to a topic.

        Args:
            cmd: Batch publish command

        Returns:
            List of published message IDs
        """
        if not self._publisher:
            msg = "Producer not initialized. Call connect() first."
            raise RuntimeError(msg)

        # For now, batch publishing isn't commonly used, so we'll implement a basic version
        # that just calls publish() for each message
        if hasattr(cmd, "messages") and cmd.messages:
            message_ids = []
            for msg in cmd.messages:
                # Create a proper GCPPublishCommand
                single_cmd = GCPPublishCommand(
                    message=msg,
                    topic=getattr(cmd, "topic", ""),
                    attributes=getattr(cmd, "attributes", {}),
                    ordering_key=getattr(cmd, "ordering_key", None),
                    correlation_id=gen_cor_id(),
                )
                msg_id = await self.publish(single_cmd)
                message_ids.append(msg_id)
            return message_ids
        return []

    async def _ensure_topic_exists(self, topic_path: str) -> None:
        """Ensure the topic exists, create if it doesn't.

        Args:
            topic_path: Full topic path (projects/project-id/topics/topic-name)
        """
        # Try to create the topic - this will fail silently if it already exists
        # GCP Pub/Sub emulator returns 409 if topic exists, production returns different errors
        with contextlib.suppress(Exception):  # nosec B110
            await self.get_publisher().create_topic(topic_path)

    def get_publisher(self) -> PublisherClient:
        """Returns the publisher and errors if does not exist."""
        assert self._publisher is not None
        return self._publisher

    async def request(
        self,
        cmd: GCPPublishCommand,
    ) -> Any:
        """Send a request and wait for response (not directly supported in Pub/Sub).

        Args:
            cmd: Request command with message, topic, and options

        Returns:
            Response data

        Raises:
            NotImplementedError: Request-reply pattern requires custom implementation
        """
        msg = (
            "Request-reply pattern is not natively supported in GCP Pub/Sub. "
            "Consider implementing using correlation IDs and a response subscription."
        )
        raise NotImplementedError(msg)
