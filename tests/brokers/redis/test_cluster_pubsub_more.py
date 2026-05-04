import asyncio
from unittest.mock import MagicMock

import pytest

from faststream.redis import RedisClusterBroker

from .basic import RedisClusterTestcaseConfig
from .conftest import SettingsCluster


@pytest.mark.connected()
@pytest.mark.redis_cluster()
class TestClusterPubSubMore(RedisClusterTestcaseConfig):
    """Additional Pub/Sub tests for RedisClusterBroker."""

    timeout: float = 10.0

    @pytest.mark.asyncio()
    async def test_pattern_subscribe(
        self,
        settings_cluster: SettingsCluster,
    ) -> None:
        broker = RedisClusterBroker(
            url=settings_cluster.url,
            startup_nodes=settings_cluster.startup_nodes,
        )
        event = asyncio.Event()
        received = []

        @broker.subscriber(channel="test.pattern.*")
        async def handler(msg: str) -> None:
            received.append(msg)
            if len(received) == 2:
                event.set()

        async with broker:
            await broker.start()
            await broker.publish("a", channel="test.pattern.foo")
            await broker.publish("b", channel="test.pattern.bar")
            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=self.timeout,
            )

        assert received == ["a", "b"]

    @pytest.mark.asyncio()
    async def test_multiple_subscribers_same_channel(
        self,
        settings_cluster: SettingsCluster,
    ) -> None:
        broker = RedisClusterBroker(
            url=settings_cluster.url,
            startup_nodes=settings_cluster.startup_nodes,
        )
        event1 = asyncio.Event()
        event2 = asyncio.Event()
        received1 = []
        received2 = []

        @broker.subscriber(channel="multi-channel")
        async def handler1(msg: str) -> None:
            received1.append(msg)
            event1.set()

        @broker.subscriber(channel="multi-channel")
        async def handler2(msg: str) -> None:
            received2.append(msg)
            event2.set()

        async with broker:
            await broker.start()
            await broker.publish("shared", channel="multi-channel")
            await asyncio.wait(
                (
                    asyncio.create_task(event1.wait()),
                    asyncio.create_task(event2.wait()),
                ),
                timeout=self.timeout,
            )

        assert received1 == ["shared"]
        assert received2 == ["shared"]

    @pytest.mark.asyncio()
    async def test_get_one_from_channel(
        self,
        settings_cluster: SettingsCluster,
        mock: MagicMock,
    ) -> None:
        broker = RedisClusterBroker(
            url=settings_cluster.url,
            startup_nodes=settings_cluster.startup_nodes,
        )

        sub = broker.subscriber(channel="get-one-channel")

        async with broker:
            await broker.start()

            async def consume() -> None:
                msg = await sub.get_one(timeout=self.timeout)
                if msg is not None:
                    mock(await msg.decode())

            async def publish() -> None:
                await asyncio.sleep(0.1)
                await broker.publish("test_msg", channel="get-one-channel")

            await asyncio.wait(
                (
                    asyncio.create_task(consume()),
                    asyncio.create_task(publish()),
                ),
                timeout=self.timeout,
            )

        mock.assert_called_once_with("test_msg")

    @pytest.mark.asyncio()
    async def test_get_one_timeout_channel(
        self,
        settings_cluster: SettingsCluster,
        mock: MagicMock,
    ) -> None:
        broker = RedisClusterBroker(
            url=settings_cluster.url,
            startup_nodes=settings_cluster.startup_nodes,
        )

        sub = broker.subscriber(channel="empty-channel")

        async with broker:
            await broker.start()
            result = await sub.get_one(timeout=0.5)
            mock(result)

        mock.assert_called_once_with(None)

    @pytest.mark.asyncio()
    async def test_headers_propagation(
        self,
        settings_cluster: SettingsCluster,
        mock: MagicMock,
    ) -> None:
        from faststream import Context

        broker = RedisClusterBroker(
            url=settings_cluster.url,
            startup_nodes=settings_cluster.startup_nodes,
            apply_types=True,
        )
        event = asyncio.Event()

        @broker.subscriber(channel="headers-channel")
        async def handler(
            body: str,
            msg=Context("message"),
        ) -> None:
            mock(
                body=body,
                correlation_id=msg.correlation_id,
                reply_to=msg.reply_to,
            )
            event.set()

        async with broker:
            await broker.start()
            await broker.publish(
                "data",
                channel="headers-channel",
                correlation_id="my-cor-id",
                reply_to="reply-chan",
            )
            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert mock.call_args[1]["body"] == "data"
        assert mock.call_args[1]["correlation_id"] == "my-cor-id"
        assert mock.call_args[1]["reply_to"] == "reply-chan"

    @pytest.mark.asyncio()
    async def test_stop_consume_channel(
        self,
        settings_cluster: SettingsCluster,
        mock: MagicMock,
    ) -> None:
        from faststream.exceptions import StopConsume

        broker = RedisClusterBroker(
            url=settings_cluster.url,
            startup_nodes=settings_cluster.startup_nodes,
        )
        event = asyncio.Event()

        @broker.subscriber(channel="stop-channel")
        async def handler(msg: str) -> None:
            mock(msg)
            event.set()
            raise StopConsume

        async with broker:
            await broker.start()
            await broker.publish("hello", channel="stop-channel")
            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=self.timeout,
            )
            await asyncio.sleep(0.5)
            await broker.publish("hello2", channel="stop-channel")
            await asyncio.sleep(0.5)

        assert event.is_set()
        mock.assert_called_once_with("hello")

    @pytest.mark.asyncio()
    async def test_unsubscribe_cleans_up(
        self,
        settings_cluster: SettingsCluster,
    ) -> None:
        broker = RedisClusterBroker(
            url=settings_cluster.url,
            startup_nodes=settings_cluster.startup_nodes,
        )
        event = asyncio.Event()

        sub = broker.subscriber(channel="restart-channel")

        @sub
        async def handler(msg: str) -> None:
            event.set()

        async with broker:
            await broker.start()

            # First round
            await sub.start()
            await broker.publish("msg1", channel="restart-channel")
            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=self.timeout,
            )
            assert event.is_set()

            await sub.stop()

            # Second round
            event.clear()
            await sub.start()
            await broker.publish("msg2", channel="restart-channel")
            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=self.timeout,
            )
            assert event.is_set()

            await sub.stop()
