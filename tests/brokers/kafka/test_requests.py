import asyncio
from typing import Any

import pytest

from faststream import BaseMiddleware, Context
from tests.brokers.base.requests import RequestsTestcase

from .basic import KafkaMemoryTestcaseConfig, KafkaTestcaseConfig


class Mid(BaseMiddleware):
    async def on_receive(self) -> None:
        self.msg.value *= 2

    async def consume_scope(self, call_next, msg):
        msg.body *= 2
        return await call_next(msg)


@pytest.mark.kafka()
@pytest.mark.asyncio()
class TestRequestTestClient(KafkaMemoryTestcaseConfig, RequestsTestcase):
    def get_middleware(self, **kwargs: Any):
        return Mid

    async def test_skip_none_request(self, queue: str) -> None:
        """A `None` request is skipped before the producer is reached.

        Without `skip_none`, `request(None)` would still send a request with
        a `null` body. With the flag enabled it must return `None`
        immediately and never invoke the subscriber handler.
        """
        broker = self.get_broker()

        called = asyncio.Event()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg) -> str:
            called.set()
            return "Response"

        publisher = broker.publisher(queue, skip_none=True)

        async with self.patch_broker(broker):
            await broker.start()

            response = await publisher.request(None, timeout=self.timeout)

        assert response is None
        assert not called.is_set()

    async def test_skip_none_batch_request(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg) -> str:
            return "Response"

        publisher = broker.publisher(queue, batch=True, skip_none=True)

        async with self.patch_broker(broker):
            await broker.start()

            # Batch publishing is not supported for request.
            response = await publisher.request(None, timeout=self.timeout)

        assert response is None

    async def test_request_without_skip_none_returns_response(self, queue: str) -> None:
        """Guard the default path: without `skip_none` RPC still works.

        Duplicates `test_publisher_base_request`, but explicitly pins the
        flag to `False` so a skip-guard regression is caught here.
        """
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg) -> str:
            return "Response"

        publisher = broker.publisher(queue, skip_none=False)

        async with self.patch_broker(broker):
            await broker.start()

            response = await publisher.request(None, timeout=self.timeout)

        assert await response.decode() == "Response"


@pytest.mark.kafka()
@pytest.mark.connected()
class TestRequestSkipNoneConnected(KafkaTestcaseConfig):
    @pytest.mark.asyncio()
    async def test_request_skips_none(self, queue: str) -> None:
        """`skip_none` is checked in `_basic_request` before the producer.

        Kafka doesn't support RPC: `AioKafkaFastProducer.request` raises
        `FeatureNotSupportedException`. This connected test passes only
        because the guard short-circuits inside the shared publisher
        pipeline (`PublisherUsecase._basic_request`), so the producer (and
        its exception) is never reached. Moving the guard into the producer
        would break this test — which is exactly the invariant it pins.
        """
        pub_broker = self.get_broker(apply_types=True)

        values: asyncio.Queue[bytes | None] = asyncio.Queue()

        @pub_broker.subscriber(queue)
        async def handler(msg: Any = Context("message")) -> None:
            await values.put(msg.raw_message.value)

        publisher = pub_broker.publisher(queue, skip_none=True)

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            result = await publisher.request(None, timeout=self.timeout)

            assert result is None

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(values.get(), timeout=1.0)
