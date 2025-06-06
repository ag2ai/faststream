import asyncio
import itertools
from unittest.mock import MagicMock, call, patch

import pytest
from redis.asyncio import Redis

from faststream import Context
from faststream.exceptions import SetupError
from faststream.redis import ListSub, RedisBroker, RedisResponse, StreamSub
from faststream.redis.annotations import Pipeline
from tests.brokers.base.publish import BrokerPublishTestcase
from tests.tools import spy_decorator


@pytest.mark.redis
@pytest.mark.asyncio
class TestPublish(BrokerPublishTestcase):
    def get_broker(self, apply_types: bool = False):
        return RedisBroker(apply_types=apply_types)

    async def test_list_publisher(
        self,
        queue: str,
        event: asyncio.Event,
        mock: MagicMock,
    ):
        pub_broker = self.get_broker()

        @pub_broker.subscriber(list=queue)
        @pub_broker.publisher(list=queue + "resp")
        async def m(msg):
            return ""

        @pub_broker.subscriber(list=queue + "resp")
        async def resp(msg):
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
    ):
        pub_broker = self.get_broker()

        msgs_queue = asyncio.Queue(maxsize=2)

        @pub_broker.subscriber(list=queue)
        async def handler(msg):
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
        self,
        queue: str,
        event: asyncio.Event,
        mock: MagicMock,
    ):
        pub_broker = self.get_broker()

        batch_list = ListSub(queue + "resp", batch=True)

        @pub_broker.subscriber(list=queue)
        @pub_broker.publisher(list=batch_list)
        async def m(msg):
            return 1, 2, 3

        @pub_broker.subscriber(list=batch_list)
        async def resp(msg):
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
        self,
        queue: str,
        event: asyncio.Event,
        mock: MagicMock,
    ):
        pub_broker = self.get_broker()

        stream = StreamSub(queue + "resp", maxlen=1)

        @pub_broker.subscriber(stream=queue)
        @pub_broker.publisher(stream=stream)
        async def handler(msg):
            return msg

        @pub_broker.subscriber(stream=stream)
        async def resp(msg):
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
        self,
        queue: str,
        event: asyncio.Event,
        mock: MagicMock,
    ):
        pub_broker = self.get_broker(apply_types=True)

        @pub_broker.subscriber(list=queue)
        @pub_broker.publisher(list=queue + "resp")
        async def m():
            return RedisResponse(1, correlation_id="1")

        @pub_broker.subscriber(list=queue + "resp")
        async def resp(msg=Context("message")):
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

    @pytest.mark.asyncio
    async def test_response_for_rpc(
        self,
        queue: str,
        event: asyncio.Event,
    ):
        pub_broker = self.get_broker(apply_types=True)

        @pub_broker.subscriber(queue)
        async def handle():
            return RedisResponse("Hi!", correlation_id="1")

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            response = await asyncio.wait_for(
                br.publish("", queue, rpc=True),
                timeout=3,
            )

            assert response == "Hi!", response

    @pytest.mark.asyncio
    @pytest.mark.pipe
    @pytest.mark.parametrize("type_queue", ["channel", "list", "stream"])
    async def test_pipeline(self, type_queue: str, queue: str, mock: MagicMock) -> None:
        event = asyncio.Event()
        pub_broker = self.get_broker(apply_types=True)
        publisher = pub_broker.publisher(**{type_queue: queue + "resp"})
        total_calls = 0

        @pub_broker.subscriber(**{type_queue: queue})
        async def m(msg: str, pipe: Pipeline) -> None:
            nonlocal total_calls

            cnt = itertools.count(0)
            for _ in range(5):
                await publisher.publish(f"hello {cnt.__next__()}", pipeline=pipe)
                await pub_broker.publish(
                    f"hello {cnt.__next__()}",
                    **{type_queue: queue + "resp"},
                    pipeline=pipe,
                )

            res = await pipe.execute()
            total_calls = len(res)

        @pub_broker.subscriber(**{type_queue: queue + "resp"})
        async def resp(msg: str) -> None:
            mock(msg)
            if msg == "hello 9":
                event.set()

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            tasks = (
                asyncio.create_task(br.publish("", **{type_queue: queue})),
                asyncio.create_task(event.wait()),
            )
            await asyncio.wait(tasks, timeout=3)

        assert event.is_set()
        assert total_calls == mock.call_count == 10
        mock.assert_has_calls([call(f"hello {i}") for i in range(10)])

    @pytest.mark.asyncio
    @pytest.mark.pipe
    async def test_pipebatch(self, queue: str, mock: MagicMock) -> None:
        event = asyncio.Event()
        pub_broker = self.get_broker(apply_types=True)
        total_calls = 0

        @pub_broker.subscriber(channel=queue)
        async def m(msg: str, pipe: Pipeline) -> None:
            nonlocal total_calls

            cnt = itertools.count(0)
            for i in range(5):
                msgs = (f"hello {cnt.__next__()}" for _ in "12")
                await pub_broker.publish_batch(
                    *msgs, list=queue + "resp", pipeline=pipe
                )

            res = await pipe.execute()
            total_calls = len(res)

        @pub_broker.subscriber(list=ListSub(queue + "resp", batch=True))
        async def resp(msg: list[str]) -> None:
            mock(msg)
            if msg[-1] == "hello 9":
                event.set()

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            tasks = (
                asyncio.create_task(br.publish("", channel=queue)),
                asyncio.create_task(event.wait()),
            )
            await asyncio.wait(tasks, timeout=3)

        assert event.is_set()
        assert total_calls == 5
        assert mock.call_count == 1
        mock.assert_called_once_with([f"hello {i}" for i in range(10)])

    @pytest.mark.asyncio
    @pytest.mark.pipe
    async def test_pipeline_both_delivery(self, queue: str) -> None:
        pub_broker = self.get_broker(apply_types=True)

        async with self.patch_broker(pub_broker) as br:
            await br.connect()

            async with br._connection.pipeline() as pipe:
                pubs = (
                    br.publish("", queue, pipeline=pipe, rpc=True),
                    br.publisher(queue).publish("", pipeline=pipe, rpc=True),
                )
                for pub in pubs:
                    with pytest.raises(
                        (RuntimeError, TimeoutError), match=r"^You cannot use both"
                    ):
                        await asyncio.wait_for(pub, timeout=0.15)
