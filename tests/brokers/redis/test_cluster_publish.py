import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest

from faststream import Context
from faststream.redis import RedisResponse
from tests.brokers.base.publish import BrokerPublishTestcase

from .basic import RedisClusterTestcaseConfig


@pytest.mark.connected()
@pytest.mark.redis_cluster()
@pytest.mark.asyncio()
class TestClusterPublish(RedisClusterTestcaseConfig, BrokerPublishTestcase):
    """Publisher tests for RedisClusterBroker (real cluster)."""

    timeout: float = 10.0

    async def test_list_publisher(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()
        pub_broker = self.get_broker()

        @pub_broker.subscriber(list=queue)
        @pub_broker.publisher(list=queue + "resp")
        async def m(_) -> str:
            return ""

        @pub_broker.subscriber(list=queue + "resp")
        async def resp(msg) -> None:
            event.set()
            mock(msg)

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", list=queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_once_with("")

    async def test_list_publish_batch(
        self,
        queue: str,
    ) -> None:
        pub_broker = self.get_broker()
        msgs_queue = asyncio.Queue(maxsize=2)

        @pub_broker.subscriber(list=queue)
        async def handler(msg) -> None:
            await msgs_queue.put(msg)

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            await br.publish_batch(1, "hi", list=queue)
            result, _ = await asyncio.wait(
                (
                    asyncio.create_task(msgs_queue.get()),
                    asyncio.create_task(msgs_queue.get()),
                ),
                timeout=self.timeout,
            )

        assert {1, "hi"} == {r.result() for r in result}

    async def test_response(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()
        pub_broker = self.get_broker(apply_types=True)

        @pub_broker.subscriber(list=queue)
        @pub_broker.publisher(list=queue + "resp")
        async def m() -> RedisResponse:
            return RedisResponse(1, correlation_id="1")

        @pub_broker.subscriber(list=queue + "resp")
        async def resp(msg=Context("message")) -> None:
            mock(body=msg.body, correlation_id=msg.correlation_id)
            event.set()

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", list=queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_once_with(body=b"1", correlation_id="1")

    async def test_response_for_rpc(self, queue: str) -> None:
        pub_broker = self.get_broker()

        @pub_broker.subscriber(queue)
        async def handle(msg: Any) -> RedisResponse:
            return RedisResponse("Hi!", correlation_id="1")

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            response = await asyncio.wait_for(
                br.request("", queue),
                timeout=self.timeout,
            )
            assert await response.decode() == "Hi!", response

    async def test_pipeline_warns(
        self,
        queue: str,
    ) -> None:
        """Pipeline should emit RuntimeWarning in cluster mode."""
        broker = self.get_broker()

        with pytest.warns(RuntimeWarning, match="Pipeline is not supported"):
            await broker.publish("hello", channel=queue, pipeline=None)  # type: ignore[arg-type]

    async def test_publish_batch_with_pipeline_warns(
        self,
        queue: str,
    ) -> None:
        """Pipeline should emit RuntimeWarning for publish_batch in cluster."""
        broker = self.get_broker()

        with pytest.warns(RuntimeWarning, match="Pipeline is not supported"):
            await broker.publish_batch("x", "y", list=queue, pipeline=None)  # type: ignore[arg-type]

    async def test_channel_publish(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """Publish to channel via cluster Pub/Sub."""
        event = asyncio.Event()
        pub_broker = self.get_broker()

        @pub_broker.subscriber(channel=queue)
        async def handler(msg) -> None:
            mock(msg)
            event.set()

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", channel=queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_once_with("hello")

    async def test_channel_publish_with_headers(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """Headers and correlation_id are propagated via Pub/Sub."""
        event = asyncio.Event()
        pub_broker = self.get_broker(apply_types=True)

        @pub_broker.subscriber(channel=queue)
        async def handler(msg, ctx_msg=Context("message")) -> None:
            mock(
                body=msg,
                correlation_id=ctx_msg.correlation_id,
            )
            event.set()

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(
                        br.publish(
                            "hi",
                            channel=queue,
                            correlation_id="cor123",
                            headers={"custom": "value"},
                        ),
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert mock.call_args[1]["body"] == "hi"
        assert mock.call_args[1]["correlation_id"] == "cor123"
