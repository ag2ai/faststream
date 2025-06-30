import asyncio

import anyio
import pytest

from .basic import BaseTestcaseConfig


class RequestsTestcase(BaseTestcaseConfig):
    def get_middleware(self, **kwargs):
        raise NotImplementedError

    def get_router(self, **kwargs):
        raise NotImplementedError

    async def test_request_timeout(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg) -> str:
            await anyio.sleep(0.01)
            return "Response"

        async with self.patch_broker(broker):
            await broker.start()

            with pytest.raises((TimeoutError, asyncio.TimeoutError)):
                await broker.request(
                    None,
                    queue,
                    timeout=1e-24,
                )

    async def test_broker_base_request(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg) -> str:
            return "Response"

        async with self.patch_broker(broker):
            await broker.start()

            response = await broker.request(
                None,
                queue,
                timeout=self.timeout,
                correlation_id="1",
            )

        assert await response.decode() == "Response"
        assert response.correlation_id == "1", response.correlation_id

    async def test_publisher_base_request(self, queue: str) -> None:
        broker = self.get_broker()

        publisher = broker.publisher(queue)

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg) -> str:
            return "Response"

        async with self.patch_broker(broker):
            await broker.start()

            response = await publisher.request(
                None,
                timeout=self.timeout,
                correlation_id="1",
            )

        assert await response.decode() == "Response"
        assert response.correlation_id == "1", response.correlation_id

    async def test_router_publisher_request(self, queue: str) -> None:
        router = self.get_router()

        publisher = router.publisher(queue)

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def handler(msg) -> str:
            return "Response"

        broker = self.get_broker()
        broker.include_router(router)

        async with self.patch_broker(broker):
            await broker.start()

            response = await publisher.request(
                None,
                timeout=self.timeout,
                correlation_id="1",
            )

        assert await response.decode() == "Response"
        assert response.correlation_id == "1", response.correlation_id

    async def test_broker_request_respect_middleware(self, queue: str) -> None:
        broker = self.get_broker(middlewares=(self.get_middleware(),))

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg):
            return msg

        async with self.patch_broker(broker):
            await broker.start()

            response = await broker.request(
                "x",
                queue,
                timeout=self.timeout,
            )

        assert await response.decode() == "x" * 2 * 2 * 2 * 2

    async def test_broker_publisher_request_respect_middleware(
        self, queue: str
    ) -> None:
        broker = self.get_broker(middlewares=(self.get_middleware(),))

        publisher = broker.publisher(queue)

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg):
            return msg

        async with self.patch_broker(broker):
            await broker.start()

            response = await publisher.request(
                "x",
                timeout=self.timeout,
            )

        assert await response.decode() == "x" * 2 * 2 * 2 * 2

    async def test_router_publisher_request_respect_middleware(
        self, queue: str
    ) -> None:
        router = self.get_router(middlewares=(self.get_middleware(),))

        publisher = router.publisher(queue)

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def handler(msg):
            return msg

        broker = self.get_broker()
        broker.include_router(router)

        async with self.patch_broker(broker):
            await broker.start()

            response = await publisher.request(
                "x",
                timeout=self.timeout,
            )

        assert await response.decode() == "x" * 2 * 2 * 2 * 2
