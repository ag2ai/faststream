"""Integration tests for RedisClusterBroker.

Requires a running Redis Cluster (ports 7001-7003).
Start with: ``just redis-cluster-up``
Run with: ``just test-redis-cluster tests/brokers/redis/test_cluster_connect.py``
"""

import asyncio
from typing import Any

import pytest

from faststream.redis import RedisClusterBroker, StreamSub
from tests.brokers.base.connection import BrokerConnectionTestcase

from .settings import SettingsCluster


@pytest.mark.connected()
@pytest.mark.redis_cluster()
class TestClusterConnection(BrokerConnectionTestcase):
    broker = RedisClusterBroker

    def get_broker_args(self, settings: SettingsCluster) -> dict[str, Any]:
        return {"url": settings.url, "startup_nodes": settings.startup_nodes}

    @pytest.mark.asyncio()
    async def test_connect(  # type: ignore[override]
        self,
        settings_cluster: SettingsCluster,
    ) -> None:
        kwargs = self.get_broker_args(settings_cluster)
        broker = self.broker(**kwargs)
        await broker.connect()
        assert await self.ping(broker)
        await broker.stop()

    @pytest.mark.asyncio()
    async def test_init_connect_by_raw_data(
        self,
        settings_cluster: SettingsCluster,
    ) -> None:
        async with RedisClusterBroker(
            "redis://127.0.0.1:63791",
            host=settings_cluster.host,
            port=settings_cluster.port,
            startup_nodes=settings_cluster.startup_nodes,
        ) as broker:
            assert await self.ping(broker)


@pytest.mark.connected()
@pytest.mark.redis_cluster()
class TestClusterList:
    @pytest.mark.asyncio()
    async def test_list_subscriber(
        self,
        queue: str,
        settings_cluster: SettingsCluster,
    ) -> None:
        broker = RedisClusterBroker(
            url=settings_cluster.url,
            startup_nodes=settings_cluster.startup_nodes,
        )
        event = asyncio.Event()
        received = []

        @broker.subscriber(list=f"test-cluster-list-{queue}")
        async def handler(msg: str) -> None:
            received.append(msg)
            if len(received) == 2:
                event.set()

        async with broker:
            await broker.start()
            await broker.publish("a", list=f"test-cluster-list-{queue}")
            await broker.publish("b", list=f"test-cluster-list-{queue}")
            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=5,
            )

        assert received == ["a", "b"]

    @pytest.mark.asyncio()
    async def test_publish_batch(
        self,
        queue: str,
        settings_cluster: SettingsCluster,
    ) -> None:
        broker = RedisClusterBroker(
            url=settings_cluster.url,
            startup_nodes=settings_cluster.startup_nodes,
        )
        event = asyncio.Event()
        received = []

        @broker.subscriber(list=f"test-cluster-batch-{queue}")
        async def handler(msg: str) -> None:
            received.append(msg)
            if len(received) == 3:
                event.set()

        async with broker:
            await broker.start()
            await broker.publish_batch("x", "y", "z", list=f"test-cluster-batch-{queue}")
            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=5,
            )

        assert received == ["x", "y", "z"]


@pytest.mark.connected()
@pytest.mark.redis_cluster()
class TestClusterStream:
    @pytest.mark.asyncio()
    async def test_stream_consume(
        self,
        queue: str,
        settings_cluster: SettingsCluster,
    ) -> None:
        broker = RedisClusterBroker(
            url=settings_cluster.url,
            startup_nodes=settings_cluster.startup_nodes,
        )
        event = asyncio.Event()
        received = []

        @broker.subscriber(stream=f"test-stream-{queue}")
        async def handler(msg: str) -> None:
            received.append(msg)
            event.set()

        async with broker:
            await broker.start()
            for _ in range(10):
                await broker.publish("hello", stream=f"test-stream-{queue}")
                done, _ = await asyncio.wait(
                    (asyncio.create_task(event.wait()),),
                    timeout=1.0,
                )
                if done:
                    break

        assert received == ["hello"]

    @pytest.mark.asyncio()
    async def test_stream_consume_group(
        self,
        queue: str,
        settings_cluster: SettingsCluster,
    ) -> None:
        broker = RedisClusterBroker(
            url=settings_cluster.url,
            startup_nodes=settings_cluster.startup_nodes,
        )
        event = asyncio.Event()
        received = []

        @broker.subscriber(
            stream=StreamSub(
                f"test-stream-group-{queue}",
                group=f"group-{queue}",
                consumer=f"consumer-{queue}",
            ),
        )
        async def handler(msg: str) -> None:
            received.append(msg)
            event.set()

        async with broker:
            await broker.start()
            await broker.publish("group msg", stream=f"test-stream-group-{queue}")
            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=5,
            )

        assert received == ["group msg"]


@pytest.mark.connected()
@pytest.mark.redis_cluster()
class TestClusterStreamAutoclaim:
    @pytest.mark.asyncio()
    async def test_consume_stream_with_min_idle_time(
        self,
        queue: str,
        settings_cluster: SettingsCluster,
    ) -> None:
        """min_idle_time subscriber uses XAUTOCLAIM to reclaim pending messages."""
        stream = f"test-autoclaim-{queue}"
        group = f"group-{queue}"

        broker = RedisClusterBroker(
            url=settings_cluster.url,
            startup_nodes=settings_cluster.startup_nodes,
        )

        # Create stream and pending messages directly via the cluster client
        async with broker:
            await broker.start()
            c = broker.config.broker_config.connection.client

            await c.xadd(stream, {"data": b"m1"})
            await c.xadd(stream, {"data": b"m2"})
            await c.xgroup_create(stream, group, id="0", mkstream=True)

            # Read without ACKing → messages go to PEL
            await c.xreadgroup(group, "temp", {stream: ">"}, count=2)

        # Now start a claimer with min_idle_time
        claimer = RedisClusterBroker(
            url=settings_cluster.url,
            startup_nodes=settings_cluster.startup_nodes,
        )
        event = asyncio.Event()
        claimed = []

        @claimer.subscriber(
            stream=StreamSub(
                stream,
                group=group,
                consumer="claimer",
                min_idle_time=100,
            ),
        )
        async def handler(msg: str) -> None:
            claimed.append(msg)
            if len(claimed) == 2:
                event.set()

        async with claimer:
            await claimer.start()
            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=10,
            )

        assert len(claimed) == 2

    @pytest.mark.slow()
    @pytest.mark.asyncio()
    async def test_xautoclaim_circular_scanning(
        self,
        queue: str,
        settings_cluster: SettingsCluster,
    ) -> None:
        """XAUTOCLAIM cursor wraps back to 0-0 at end of PEL."""
        stream = f"test-circ-{queue}"
        group = f"group-{queue}"

        broker = RedisClusterBroker(
            url=settings_cluster.url,
            startup_nodes=settings_cluster.startup_nodes,
        )

        async with broker:
            await broker.start()
            c = broker.config.broker_config.connection.client

            for i in range(5):
                await c.xadd(stream, {"data": f"msg-{i}".encode()})
            await c.xgroup_create(stream, group, id="0", mkstream=True)
            await c.xreadgroup(group, "temp", {stream: ">"}, count=5)

        claimer = RedisClusterBroker(
            url=settings_cluster.url,
            startup_nodes=settings_cluster.startup_nodes,
        )
        claimed = []

        @claimer.subscriber(
            stream=StreamSub(
                stream,
                group=group,
                consumer="claimer",
                min_idle_time=100,
            ),
        )
        async def handler(msg: str) -> None:
            claimed.append(msg)

        async with claimer:
            await claimer.start()
            await asyncio.sleep(5)

        assert len(claimed) >= 5


@pytest.mark.connected()
@pytest.mark.redis_cluster()
class TestClusterPubSub:
    @pytest.mark.asyncio()
    async def test_publish_subscribe(
        self,
        queue: str,
        settings_cluster: SettingsCluster,
    ) -> None:
        """Pub/Sub via sync cluster wrapper."""
        from redis.cluster import RedisCluster as _SyncRC

        if not hasattr(_SyncRC, "publish"):
            pytest.skip("sync cluster pubsub not available")

        broker = RedisClusterBroker(
            url=settings_cluster.url,
            startup_nodes=settings_cluster.startup_nodes,
        )
        event = asyncio.Event()
        received = []

        @broker.subscriber(f"test-pubsub-{queue}")
        async def handler(msg: str) -> None:
            received.append(msg)
            event.set()

        async with broker:
            await broker.start()
            await broker.publish("hello", channel=f"test-pubsub-{queue}")
            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=5,
            )

        assert received == ["hello"]
        # a future redis-py version fixes this the test can be written
        # analogously to test_publish_subscribe with `sharded=True`.
