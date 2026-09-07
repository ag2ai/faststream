import asyncio
from unittest.mock import MagicMock

import pytest

from tests.brokers.redis.basic import RedisClusterTestcaseConfig

from .settings import SettingsCluster


@pytest.mark.slow()
@pytest.mark.connected()
@pytest.mark.redis_cluster()
class TestClusterPubSubMore(RedisClusterTestcaseConfig):
    """Additional Pub/Sub tests for RedisClusterBroker."""

    timeout: float = 10.0

    @pytest.mark.asyncio()
    async def test_pattern_subscribe(
        self,
        queue: str,
        settings_cluster: SettingsCluster,
        event: asyncio.Event,
    ) -> None:
        broker = self.get_broker(startup_nodes=settings_cluster.startup_nodes)

        received = []

        @broker.subscriber(channel=f"test.pattern.{queue}.*")
        async def handler(msg: str) -> None:
            received.append(msg)
            if len(received) == 2:
                event.set()

        async with broker:
            await broker.start()
            await broker.publish("a", channel=f"test.pattern.{queue}.foo")
            await broker.publish("b", channel=f"test.pattern.{queue}.bar")
            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=self.timeout,
            )

        # The two channels hash to their own slots, so the publishes reach different
        # nodes and propagate over the cluster bus independently. Which one arrives
        # first is a race, and not one this test is about — sorted rather than a set,
        # so a message delivered twice still fails.
        assert sorted(received) == ["a", "b"]

    @pytest.mark.asyncio()
    async def test_multiple_subscribers_same_channel(
        self,
        queue: str,
        settings_cluster: SettingsCluster,
        event: asyncio.Event,
        event2: asyncio.Event,
    ) -> None:
        broker = self.get_broker(startup_nodes=settings_cluster.startup_nodes)

        received1 = []
        received2 = []

        @broker.subscriber(channel=f"multi-channel-{queue}")
        async def handler1(msg: str) -> None:
            received1.append(msg)
            event.set()

        @broker.subscriber(channel=f"multi-channel-{queue}")
        async def handler2(msg: str) -> None:
            received2.append(msg)
            event2.set()

        async with broker:
            await broker.start()
            await broker.publish("shared", channel=f"multi-channel-{queue}")
            await asyncio.wait(
                (
                    asyncio.create_task(event.wait()),
                    asyncio.create_task(event2.wait()),
                ),
                timeout=self.timeout,
            )

        assert received1 == ["shared"]
        assert received2 == ["shared"]

    @pytest.mark.asyncio()
    async def test_get_one_from_channel(
        self,
        queue: str,
        settings_cluster: SettingsCluster,
        mock: MagicMock,
    ) -> None:
        broker = self.get_broker(startup_nodes=settings_cluster.startup_nodes)

        sub = broker.subscriber(channel=f"get-one-channel-{queue}")

        async with broker:
            await broker.start()

            async def consume() -> None:
                msg = await sub.get_one(timeout=self.timeout)
                if msg is not None:
                    mock(await msg.decode())

            async def publish() -> None:
                await asyncio.sleep(0.1)
                await broker.publish("test_msg", channel=f"get-one-channel-{queue}")

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
        queue: str,
        settings_cluster: SettingsCluster,
        mock: MagicMock,
    ) -> None:
        broker = self.get_broker(startup_nodes=settings_cluster.startup_nodes)

        sub = broker.subscriber(channel=f"empty-channel-{queue}")

        async with broker:
            await broker.start()
            result = await sub.get_one(timeout=0.5)
            mock(result)

        mock.assert_called_once_with(None)

    @pytest.mark.asyncio()
    async def test_headers_propagation(
        self,
        queue: str,
        settings_cluster: SettingsCluster,
        mock: MagicMock,
        event: asyncio.Event,
    ) -> None:
        from faststream import Context

        broker = self.get_broker(
            apply_types=True,
            startup_nodes=settings_cluster.startup_nodes,
        )

        @broker.subscriber(channel=f"headers-channel-{queue}")
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
                channel=f"headers-channel-{queue}",
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
        queue: str,
        settings_cluster: SettingsCluster,
        mock: MagicMock,
        event: asyncio.Event,
    ) -> None:
        from faststream.exceptions import StopConsume

        broker = self.get_broker(startup_nodes=settings_cluster.startup_nodes)

        @broker.subscriber(channel=f"stop-channel-{queue}")
        async def handler(msg: str) -> None:
            mock(msg)
            event.set()
            raise StopConsume

        async with broker:
            await broker.start()
            await broker.publish("hello", channel=f"stop-channel-{queue}")
            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=self.timeout,
            )
            await asyncio.sleep(0.5)
            await broker.publish("hello2", channel=f"stop-channel-{queue}")
            await asyncio.sleep(0.5)

        assert event.is_set()
        mock.assert_called_once_with("hello")

    @pytest.mark.asyncio()
    async def test_unsubscribe_cleans_up(
        self,
        queue: str,
        settings_cluster: SettingsCluster,
        event: asyncio.Event,
    ) -> None:
        broker = self.get_broker(startup_nodes=settings_cluster.startup_nodes)

        sub = broker.subscriber(channel=f"restart-channel-{queue}")

        @sub
        async def handler(msg: str) -> None:
            event.set()

        async with broker:
            await broker.start()

            # First round
            await sub.start()
            await broker.publish("msg1", channel=f"restart-channel-{queue}")
            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=self.timeout,
            )
            assert event.is_set()

            await sub.stop()

            # Second round
            event.clear()
            await sub.start()
            await broker.publish("msg2", channel=f"restart-channel-{queue}")
            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=self.timeout,
            )
            assert event.is_set()

            await sub.stop()
