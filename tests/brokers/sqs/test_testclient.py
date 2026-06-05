import pytest

from faststream import FastStream
from faststream.sqs import FifoQueue, SQSBroker, SQSRoute, SQSRouter, TestSQSBroker
from tests.marks import require_sqs


@require_sqs
@pytest.mark.sqs()
@pytest.mark.asyncio()
class TestSQSTestClient:
    async def test_pub_sub(self, queue: str) -> None:
        broker = SQSBroker()

        @broker.subscriber(queue)
        async def handler(msg: str) -> str:
            return msg + "-ok"

        async with TestSQSBroker(broker) as br:
            await br.publish("hello", queue)
            handler.mock.assert_called_once_with("hello")

    async def test_publisher_decorator(self, queue: str) -> None:
        broker = SQSBroker()
        out = queue + "-out"

        @broker.subscriber(out)
        async def downstream(msg: str) -> None: ...

        @broker.publisher(out)
        @broker.subscriber(queue)
        async def handler(msg: str) -> str:
            return msg.upper()

        async with TestSQSBroker(broker) as br:
            await br.publish("abc", queue)
            handler.mock.assert_called_once_with("abc")
            downstream.mock.assert_called_once_with("ABC")

    async def test_request_rpc(self, queue: str) -> None:
        broker = SQSBroker(response_queue="responses")

        @broker.subscriber(queue)
        async def handler(msg: str) -> str:
            return f"reply:{msg}"

        async with TestSQSBroker(broker) as br:
            response = await br.request("ping", queue)
            assert await response.decode() == "reply:ping"

    async def test_router_prefix(self, queue: str) -> None:
        broker = SQSBroker()
        received: list[str] = []

        async def handler(msg: str) -> None:
            received.append(msg)

        router = SQSRouter(prefix="pref-", handlers=[SQSRoute(handler, queue)])
        broker.include_router(router)

        async with TestSQSBroker(broker) as br:
            await br.publish("data", f"pref-{queue}")
            assert received == ["data"]

    async def test_fifo_group_id(self, queue: str) -> None:
        broker = SQSBroker()
        fifo = FifoQueue(name=queue)

        @broker.subscriber(fifo)
        async def handler(msg: str) -> None: ...

        async with TestSQSBroker(broker) as br:
            await br.publish("m", fifo, group_id="g1")
            handler.mock.assert_called_once_with("m")

    async def test_no_match_skips_handler(self, queue: str) -> None:
        broker = SQSBroker()

        @broker.subscriber(queue)
        async def handler(msg: str) -> None: ...

        async with TestSQSBroker(broker) as br:
            await br.publish("x", queue + "-other")
            handler.mock.assert_not_called()

    async def test_app_runs(self, queue: str) -> None:
        broker = SQSBroker()
        app = FastStream(broker)

        @broker.subscriber(queue)
        async def handler(msg: str) -> None: ...

        async with TestSQSBroker(broker):
            await broker.start()
            await broker.publish("hi", queue)
            handler.mock.assert_called_once_with("hi")
        assert app.broker is broker
