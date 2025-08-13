"""GCP Pub/Sub message consumption tests - Memory mode."""

import asyncio
from typing import Any

import pytest

from faststream.gcppubsub import GCPPubSubMessage
from tests.marks import require_gcppubsub

from .basic import GCPPubSubMemoryTestcaseConfig


@pytest.mark.gcppubsub()
@require_gcppubsub
class TestConsumeMemory(GCPPubSubMemoryTestcaseConfig):
    """Test GCP Pub/Sub message consumption in memory mode."""

    @pytest.mark.asyncio()
    async def test_consume_basic_memory(self, subscription: str, topic: str) -> None:
        """Test basic message consumption from subscription in memory mode."""
        event = asyncio.Event()
        consume_broker = self.get_broker()

        @consume_broker.subscriber(subscription, topic=topic)
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("test message", topic=topic)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_consume_with_attributes_memory(
        self, subscription: str, topic: str
    ) -> None:
        """Test message consumption and publishing with attributes in memory mode."""
        event = asyncio.Event()
        received_message = None
        consume_broker = self.get_broker()

        @consume_broker.subscriber(subscription, topic=topic)
        async def handler(msg: GCPPubSubMessage) -> None:  # Now works! 🎉
            nonlocal received_message
            received_message = msg
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(
                        br.publish(
                            "test message",
                            topic=topic,
                            attributes={"test_key": "test_value"},
                        )
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert received_message is not None
        # In simple mode, verify we received the decoded message content
        assert received_message == "test message"
