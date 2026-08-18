import asyncio
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from redis.asyncio import Redis

from faststream import Context
from faststream.redis import ListSub, Pipeline, RedisResponse, StreamSub
from tests.brokers.base.publish import BrokerPublishTestcase
from tests.tools import spy_decorator

from .basic import RedisTestcaseConfig


@pytest.mark.connected()
@pytest.mark.redis()
@pytest.mark.asyncio()
class TestPublish(RedisTestcaseConfig, BrokerPublishTestcase):
    async def test_list_publisher(
        self, queue: str, mock: MagicMock, event: asyncio.Event
    ) -> None:
        pub_broker = self.get_broker()

        @pub_broker.subscriber(list=queue)
        @pub_broker.publisher(list=queue + "resp")
        async def m(msg) -> str:
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
                timeout=3,
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
                timeout=3,
            )

        assert {1, "hi"} == {r.result() for r in result}

    async def test_batch_list_publisher(
        self, queue: str, mock: MagicMock, event: asyncio.Event
    ) -> None:
        pub_broker = self.get_broker()

        batch_list = ListSub(queue + "resp", batch=True)

        @pub_broker.subscriber(list=queue)
        @pub_broker.publisher(list=batch_list)
        async def m(msg):
            return 1, 2, 3

        @pub_broker.subscriber(list=batch_list)
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
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_once_with([1, 2, 3])

    async def test_publisher_with_maxlen(
        self, queue: str, mock: MagicMock, event: asyncio.Event
    ) -> None:
        pub_broker = self.get_broker()

        stream = StreamSub(queue + "resp", maxlen=1)

        @pub_broker.subscriber(stream=queue)
        @pub_broker.publisher(stream=stream)
        async def handler(msg):
            return msg

        @pub_broker.subscriber(stream=stream)
        async def resp(msg) -> None:
            event.set()
            mock(msg)

        with patch.object(Redis, "xadd", spy_decorator(Redis.xadd)) as m:
            async with self.patch_broker(pub_broker) as br:
                await br.start()

                await asyncio.wait(
                    (
                        asyncio.create_task(br.publish("hi", stream=queue)),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )

        assert event.is_set()
        mock.assert_called_once_with("hi")

        assert m.mock.call_args_list[-1].kwargs["maxlen"] == 1

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
            mock(
                body=msg.body,
                correlation_id=msg.correlation_id,
            )
            event.set()

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", list=queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_once_with(
            body=b"1",
            correlation_id="1",
        )

    async def test_response_for_rpc(self, queue: str) -> None:
        pub_broker = self.get_broker()

        @pub_broker.subscriber(queue)
        async def handle(msg: Any) -> RedisResponse:
            return RedisResponse("Hi!", correlation_id="1")

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            response = await asyncio.wait_for(
                br.request("", queue),
                timeout=3,
            )

            assert await response.decode() == "Hi!", response

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
        async def m(msg: str, pipe: Pipeline) -> None:
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
        async def m(msg: str, pipe: Pipeline) -> None:
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

    @pytest.mark.asyncio()
    async def test_publish_none_is_not_skipped_by_default(self, queue: str) -> None:
        pub_broker = self.get_broker(apply_types=True)

        values: asyncio.Queue[bytes | None] = asyncio.Queue()

        @pub_broker.subscriber(queue)
        async def handler(msg: Any = Context("message")) -> None:
            await values.put(msg.body)

        publisher = pub_broker.publisher(queue)

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            await publisher.publish(None)
            value = await asyncio.wait_for(values.get(), timeout=self.timeout)

        assert value == b""

    @pytest.mark.asyncio()
    async def test_publisher_skips_none(self, queue: str) -> None:
        pub_broker = self.get_broker(apply_types=True)

        values: asyncio.Queue[bytes | None] = asyncio.Queue()

        @pub_broker.subscriber(queue)
        async def handler(msg: Any = Context("message")) -> None:
            await values.put(msg.body)

        publisher = pub_broker.publisher(queue, skip_none=True)

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            result = await publisher.publish(None)

            assert result is None

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(values.get(), timeout=1.0)

    @pytest.mark.asyncio()
    async def test_batch_publish_skips_all_none_batch(self, queue: str) -> None:
        pub_broker = self.get_broker(apply_types=True)

        values: asyncio.Queue[bytes] = asyncio.Queue()

        @pub_broker.subscriber(list=ListSub(queue, batch=True))
        async def handler(msg: Any = Context("message")) -> None:
            await values.put(msg.body)

        publisher = pub_broker.publisher(
            list=ListSub(queue, batch=True), skip_none=True
        )

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            result = await publisher.publish(None, None, list=queue)

            assert result is None

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(values.get(), timeout=1.0)

    @pytest.mark.asyncio()
    async def test_batch_publish_filters_none_values(self, queue: str) -> None:
        pub_broker = self.get_broker(apply_types=True)

        values: asyncio.Queue[bytes] = asyncio.Queue()

        @pub_broker.subscriber(list=ListSub(queue, batch=True))
        async def handler(msg: Any = Context("message")) -> None:
            await values.put(msg.body)

        publisher = pub_broker.publisher(
            list=ListSub(queue, batch=True), skip_none=True
        )

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            await publisher.publish(None, "hi", list=queue)

            value = await asyncio.wait_for(values.get(), timeout=self.timeout)

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(values.get(), timeout=1.0)

        assert value == b'["hi"]'

    @pytest.mark.asyncio()
    async def test_batch_publish_without_skip_none_keeps_none_values(
        self, queue: str
    ) -> None:
        pub_broker = self.get_broker(apply_types=True)

        values: asyncio.Queue[bytes] = asyncio.Queue()

        @pub_broker.subscriber(list=ListSub(queue, batch=True))
        async def handler(msg: Any = Context("message")) -> None:
            await values.put(msg.body)

        publisher = pub_broker.publisher(list=ListSub(queue, batch=True))

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await publisher.publish(None, "hi", None, list=queue)

            value = await asyncio.wait_for(values.get(), timeout=self.timeout)

        assert value == b'["", "hi", ""]'

    @pytest.mark.asyncio()
    async def test_handler_return_none_is_skipped(self, queue: str) -> None:
        pub_broker = self.get_broker(apply_types=True)

        values: asyncio.Queue[bytes | None] = asyncio.Queue()

        @pub_broker.publisher(queue + "1", skip_none=True)
        @pub_broker.subscriber(queue)
        async def handler(msg: Any = Context("message")) -> None: ...

        @pub_broker.subscriber(queue + "1")
        async def out_handler(msg: Any = Context("message")) -> None:
            await values.put(msg.body)

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            await br.publish("test", queue)

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(values.get(), timeout=1.0)
