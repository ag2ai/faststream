import asyncio

import pytest

from faststream.mq.message import MQRawMessage

from .basic import MQMemoryTestcaseConfig


class DummyConnection:
    def __init__(self) -> None:
        self.commits = 0
        self.backouts = 0

    async def commit(self) -> None:
        self.commits += 1

    async def backout(self) -> None:
        self.backouts += 1


@pytest.mark.mq()
@pytest.mark.asyncio()
class TestTransactions(MQMemoryTestcaseConfig):
    async def test_parser_failure_backouts_message(self, queue: str) -> None:
        broker = self.get_broker()

        async def broken_parser(msg, original):
            raise ValueError("boom")

        subscriber = broker.subscriber(queue, parser=broken_parser)

        @subscriber
        async def handler(msg) -> None: ...

        connection = DummyConnection()
        raw_message = MQRawMessage(body=b"hello", queue=queue, connection=connection)

        async with self.patch_broker(broker) as br:
            await br.start()
            await subscriber.consume(raw_message)

        assert connection.commits == 0
        assert connection.backouts == 1
        assert raw_message.settled is not None

    async def test_no_handler_backouts_message(self, queue: str) -> None:
        broker = self.get_broker()

        subscriber = broker.subscriber(queue)

        @subscriber(filter=lambda msg: msg.content_type == "application/json")
        async def handler(msg) -> None: ...

        connection = DummyConnection()
        raw_message = MQRawMessage(body=b"hello", queue=queue, connection=connection)

        async with self.patch_broker(broker) as br:
            await br.start()
            await subscriber.consume(raw_message)

        assert connection.commits == 0
        assert connection.backouts == 1
        assert raw_message.settled is not None

    async def test_get_one_parser_failure_backouts_message(self, queue: str) -> None:
        async def broken_parser(msg, original):
            raise ValueError("boom")

        broker = self.get_broker(parser=broken_parser)
        subscriber = broker.subscriber(queue)
        broker.config.broker_config.producer = type(
            "TestProducer",
            (),
            {"is_test_producer": True},
        )()

        connection = DummyConnection()
        raw_message = MQRawMessage(body=b"hello", queue=queue, connection=connection)

        async with self.patch_broker(broker) as br:
            await br.start()
            subscriber._test_messages = asyncio.Queue()
            await subscriber.put_test_message(raw_message)

            with pytest.raises(ValueError, match="boom"):
                await subscriber.get_one(timeout=self.timeout)

        assert connection.commits == 0
        assert connection.backouts == 1
        assert raw_message.settled is not None
