"""Integration tests for RedisClusterBroker.

These tests require a running Redis Cluster on localhost:7000.
Start one with: docker compose up -d redis-cluster
"""

import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest

from faststream.redis import RedisClusterBroker, StreamSub
from tests.brokers.base.connection import BrokerConnectionTestcase


@pytest.mark.connected()
@pytest.mark.redis()
class TestClusterConnection(BrokerConnectionTestcase):
    broker = RedisClusterBroker

    def get_broker_args(self, settings: Any) -> dict[str, Any]:
        return {"url": "redis://localhost:7000"}

    @pytest.mark.asyncio()
    async def test_cluster_connect(self) -> None:
        async with RedisClusterBroker("redis://localhost:7000") as broker:
            assert await broker.ping(timeout=5.0)


@pytest.mark.connected()
@pytest.mark.redis()
@pytest.mark.asyncio()
class TestClusterConsume:
    async def test_cluster_stream_publish_consume(
        self,
        queue: str,
    ) -> None:
        mock = MagicMock()
        event = asyncio.Event()

        broker = RedisClusterBroker("redis://localhost:7000")

        @broker.subscriber(stream=queue)
        async def handler(msg: str) -> None:
            mock(msg)
            event.set()

        async with broker:
            await broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(broker.publish("hello", stream=queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=10,
            )

        mock.assert_called_once_with("hello")

    async def test_cluster_list_publish_consume(
        self,
        queue: str,
    ) -> None:
        mock = MagicMock()
        event = asyncio.Event()

        broker = RedisClusterBroker("redis://localhost:7000")

        @broker.subscriber(list=queue)
        async def handler(msg: str) -> None:
            mock(msg)
            event.set()

        async with broker:
            await broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(broker.publish("world", list=queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=10,
            )

        mock.assert_called_once_with("world")

    async def test_cluster_stream_with_group(
        self,
        queue: str,
    ) -> None:
        mock = MagicMock()
        event = asyncio.Event()

        broker = RedisClusterBroker("redis://localhost:7000")

        @broker.subscriber(
            stream=StreamSub(queue, group="test-group", consumer="c1"),
        )
        async def handler(msg: str) -> None:
            mock(msg)
            event.set()

        async with broker:
            await broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(broker.publish("grouped", stream=queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=10,
            )

        mock.assert_called_once_with("grouped")
