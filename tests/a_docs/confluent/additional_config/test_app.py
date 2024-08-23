import pytest

from docs.docs_src.confluent.additional_config.app import (
    HelloWorld,
    broker,
    on_hello_world,
)
from faststream.confluent import TestKafkaBroker


@pytest.mark.asyncio
async def test_base_app():
    async with TestKafkaBroker(broker):
        await broker.publish(HelloWorld(msg="First Hello"), "hello_world")
        on_hello_world.mock.assert_called_with(dict(HelloWorld(msg="First Hello")))
