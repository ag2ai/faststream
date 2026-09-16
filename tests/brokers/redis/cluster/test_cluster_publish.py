import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest

from faststream import Context
from faststream.redis import ClusterPipeline, ListSub, RedisResponse
from tests.brokers.base.publish import BrokerPublishTestcase
from tests.brokers.redis.basic import RedisClusterTestcaseConfig


@pytest.mark.connected()
@pytest.mark.redis_cluster()
@pytest.mark.asyncio()
class TestClusterPublish(RedisClusterTestcaseConfig, BrokerPublishTestcase):
    """Publisher tests for RedisClusterBroker (real cluster)."""

    timeout: float = 10.0

    @pytest.mark.slow()
    async def test_multiple_publishers(self, queue: str, mock: MagicMock) -> None:
        await super().test_multiple_publishers(queue, mock)

    @pytest.mark.slow()
    async def test_reusable_publishers(self, queue: str, mock: MagicMock) -> None:
        await super().test_reusable_publishers(queue, mock)

    @pytest.mark.slow()
    async def test_reply_to(
        self, queue: str, mock: MagicMock, event: asyncio.Event
    ) -> None:
        await super().test_reply_to(queue, mock, event)

    @pytest.mark.slow()
    async def test_no_reply(
        self, queue: str, mock: MagicMock, event: asyncio.Event
    ) -> None:
        await super().test_no_reply(queue, mock, event)

    async def test_list_publisher(
        self, queue: str, mock: MagicMock, event: asyncio.Event
    ) -> None:
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
        self, queue: str, mock: MagicMock, event: asyncio.Event
    ) -> None:
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

    @pytest.mark.slow()
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

    async def test_channel_publish(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
    ) -> None:
        """Publish to channel via cluster Pub/Sub."""
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
        event: asyncio.Event,
    ) -> None:
        """Headers and correlation_id are propagated via Pub/Sub."""
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

    @pytest.mark.asyncio()
    @pytest.mark.parametrize(
        "type_queue",
        (
            pytest.param("channel"),
            pytest.param("list"),
            pytest.param("stream"),
        ),
    )
    async def test_publish_with_pipeline(
        self,
        event: asyncio.Event,
        type_queue: str,
        queue: str,
        mock: MagicMock,
    ) -> None:
        broker = self.get_broker(apply_types=True)

        destination = {type_queue: queue + "resp"}
        publisher = broker.publisher(**destination)

        @broker.subscriber(**{type_queue: queue})
        async def m(msg: str, pipe: ClusterPipeline) -> None:
            for _ in range(5):
                # publish 5 messages by publisher
                await publisher.publish(None, pipeline=pipe)

                # and 5 by broker
                await broker.publish(None, **destination, pipeline=pipe)

            await pipe.execute()

        @broker.subscriber(**destination)
        async def resp(msg: str) -> None:
            mock(msg)
            if mock.call_count == 10:
                event.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            tasks = (
                asyncio.create_task(br.publish("", **{type_queue: queue})),
                asyncio.create_task(event.wait()),
            )
            await asyncio.wait(tasks, timeout=3)

        assert mock.call_count == 10

    @pytest.mark.asyncio()
    async def test_publish_batch_with_pipeline(
        self,
        event: asyncio.Event,
        queue: str,
        mock: MagicMock,
    ) -> None:
        broker = self.get_broker(apply_types=True)

        @broker.subscriber(channel=queue)
        async def m(msg: str, pipe: ClusterPipeline) -> None:
            await broker.publish_batch(*range(5), list=queue + "resp", pipeline=pipe)
            await pipe.execute()

        @broker.subscriber(list=ListSub(queue + "resp", batch=True, max_records=5))
        async def resp(msgs: list[int]) -> None:
            mock(msgs)
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            tasks = (
                asyncio.create_task(br.publish("", channel=queue)),
                asyncio.create_task(event.wait()),
            )
            await asyncio.wait(tasks, timeout=3)

        mock.assert_called_once_with([0, 1, 2, 3, 4])
