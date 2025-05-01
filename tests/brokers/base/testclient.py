import asyncio
from abc import abstractmethod
from unittest.mock import Mock

import anyio
import pytest

from faststream.testing.broker import TestBroker
from faststream.types import AnyCallable
from tests.brokers.base.consume import BrokerConsumeTestcase
from tests.brokers.base.publish import BrokerPublishTestcase
from tests.brokers.base.rpc import BrokerRPCTestcase


class BrokerTestclientTestcase(
    BrokerPublishTestcase,
    BrokerConsumeTestcase,
    BrokerRPCTestcase,
):
    build_message: AnyCallable
    test_class: TestBroker

    @abstractmethod
    def get_fake_producer_class(self) -> type:
        raise NotImplementedError

    @pytest.mark.asyncio
    async def test_subscriber_mock(self, queue: str):
        test_broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @test_broker.subscriber(*args, **kwargs)
        async def m(msg):
            pass

        async with self.test_class(test_broker):
            await test_broker.start()
            await test_broker.publish("hello", queue)
            m.mock.assert_called_once_with("hello")

    @pytest.mark.asyncio
    async def test_publisher_mock(self, queue: str):
        test_broker = self.get_broker()

        publisher = test_broker.publisher(queue + "resp")

        args, kwargs = self.get_subscriber_params(queue)

        @publisher
        @test_broker.subscriber(*args, **kwargs)
        async def m(msg):
            return "response"

        async with self.test_class(test_broker):
            await test_broker.start()
            await test_broker.publish("hello", queue)
            publisher.mock.assert_called_with("response")

    @pytest.mark.asyncio
    async def test_publisher_with_subscriber__mock(self, queue: str):
        test_broker = self.get_broker()

        publisher = test_broker.publisher(queue + "resp")

        args, kwargs = self.get_subscriber_params(queue)

        @publisher
        @test_broker.subscriber(*args, **kwargs)
        async def m(msg):
            return "response"

        args2, kwargs2 = self.get_subscriber_params(queue + "resp")

        @test_broker.subscriber(*args2, **kwargs2)
        async def handler_response(msg): ...

        async with self.test_class(test_broker):
            await test_broker.start()

            assert len(test_broker._subscribers) == 2

            await test_broker.publish("hello", queue)
            publisher.mock.assert_called_with("response")
            handler_response.mock.assert_called_once_with("response")

    @pytest.mark.asyncio
    async def test_manual_publisher_mock(self, queue: str):
        test_broker = self.get_broker()

        publisher = test_broker.publisher(queue + "resp")

        args, kwargs = self.get_subscriber_params(queue)

        @test_broker.subscriber(*args, **kwargs)
        async def m(msg):
            await publisher.publish("response")

        async with self.test_class(test_broker):
            await test_broker.start()
            await test_broker.publish("hello", queue)
            publisher.mock.assert_called_with("response")

    @pytest.mark.asyncio
    async def test_exception_raises(self, queue: str):
        test_broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @test_broker.subscriber(*args, **kwargs)
        async def m(msg):  # pragma: no cover
            raise ValueError()

        async with self.test_class(test_broker):
            await test_broker.start()

            with pytest.raises(ValueError):  # noqa: PT011
                await test_broker.publish("hello", queue)

    async def test_broker_gets_patched_attrs_within_cm(self):
        test_broker = self.get_broker()
        fake_producer_class = self.get_fake_producer_class()
        await test_broker.start()

        async with self.test_class(test_broker) as br:
            assert isinstance(br.start, Mock)
            assert isinstance(br._connect, Mock)
            assert isinstance(br.close, Mock)
            assert isinstance(br._producer, fake_producer_class)

        assert not isinstance(br.start, Mock)
        assert not isinstance(br._connect, Mock)
        assert not isinstance(br.close, Mock)
        assert br._connection is not None
        assert not isinstance(br._producer, fake_producer_class)

    async def test_broker_with_real_doesnt_get_patched(self):
        test_broker = self.get_broker()
        await test_broker.start()

        async with self.test_class(test_broker, with_real=True) as br:
            assert not isinstance(br.start, Mock)
            assert not isinstance(br._connect, Mock)
            assert not isinstance(br.close, Mock)
            assert br._connection is not None
            assert br._producer is not None

    async def test_broker_with_real_patches_publishers_and_subscribers(
        self, queue: str
    ):
        test_broker = self.get_broker()

        publisher = test_broker.publisher(f"{queue}1")

        args, kwargs = self.get_subscriber_params(queue)

        @test_broker.subscriber(*args, **kwargs)
        async def m(msg):
            await publisher.publish(f"response: {msg}")

        async with self.test_class(test_broker, with_real=True) as br:
            await br.publish("hello", queue)
            await m.wait_call(self.timeout)
            m.mock.assert_called_once_with("hello")

            with anyio.fail_after(self.timeout):
                while not publisher.mock.called:  # noqa: ASYNC110
                    await asyncio.sleep(0.1)

                publisher.mock.assert_called_once_with("response: hello")
