import pytest

from docs.docs_src.kafka.multiple_topics_subscription.app import (
    broker,
    on_multiple_topics,
)
from faststream.kafka import TestKafkaBroker


@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_app() -> None:
    async with TestKafkaBroker(broker):
        await broker.publish("hello", "topic1")
        on_multiple_topics.mock.assert_called_with("hello")


@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_other_topics() -> None:
    async with TestKafkaBroker(broker):
        await broker.publish("hello", "topic2")
        on_multiple_topics.mock.assert_called_with("hello")

        await broker.publish("hello", "topic3")
        on_multiple_topics.mock.assert_called_with("hello")
