import asyncio
from typing import Type
from unittest.mock import MagicMock

import pytest

from faststream._compat import json_dumps
from faststream.redis import RedisBroker, TestRedisBroker
from faststream.redis.parser import (
    BinaryMessageFormatV1,
    JSONMessageFormat,
    MessageFormat,
)
from tests.brokers.base.parser import CustomParserTestcase


@pytest.mark.redis
class TestCustomParser(CustomParserTestcase):
    broker_class = RedisBroker


@pytest.mark.parametrize(
    ("input", "should_be"),
    [
        ("", b""),
        ("plain text", b"plain text"),
        (
            {"id": "12345678" * 4, "date": "2021-01-01T00:00:00Z"},
            json_dumps({"id": "12345678" * 4, "date": "2021-01-01T00:00:00Z"}),
        ),
        (
            # UTF-8 incompitable bytes
            b"\x82\xa2id\xd9",
            b"\x82\xa2id\xd9",
        ),
    ],
)
def test_binary_message_encode_parse(input, should_be) -> None:
    raw_message = BinaryMessageFormatV1.encode(
        message=input, reply_to=None, headers=None, correlation_id="id"
    )
    parsed, _ = BinaryMessageFormatV1.parse(raw_message)
    assert parsed == should_be


@pytest.mark.parametrize(
    ("input", "should_be"),
    [
        ("", b""),
        ("plain text", b"plain text"),
        (
            {"id": "12345678" * 4, "date": "2021-01-01T00:00:00Z"},
            json_dumps({"id": "12345678" * 4, "date": "2021-01-01T00:00:00Z"}),
        ),
    ],
)
def test_json_message_encode_parse(input, should_be) -> None:
    raw_message = JSONMessageFormat.encode(
        message=input, reply_to=None, headers=None, correlation_id="id"
    )
    parsed, _ = JSONMessageFormat.parse(raw_message)
    assert parsed == should_be


def test_parse_json_with_binary_format() -> None:
    message = b'{"headers": {"correlation_id": 1, "reply_to": "service1", "content-type": "plain/text"}, "data": "hello"}'
    headers_should_be = {
        "correlation_id": 1,
        "reply_to": "service1",
        "content-type": "plain/text",
    }
    data_should_be = b"hello"
    parsed_data, parsed_headers = BinaryMessageFormatV1.parse(message)
    assert parsed_headers == headers_should_be
    assert parsed_data == data_should_be


@pytest.mark.redis
@pytest.mark.asyncio
class TestFormats:
    @pytest.mark.parametrize(
        ("message_format", "message"),
        [
            (JSONMessageFormat, b'{"data":"hello"}'),
            (
                BinaryMessageFormatV1,
                b"\x89BIN\x0d\x0a\x1a\x0a\x00\x01\x00\00\x00\x12\x00\x00\x00\x14\x00\x00hello",
            ),
        ],
    )
    async def test_consume_in_different_formats(
        self,
        queue: str,
        event: asyncio.Event,
        mock: MagicMock,
        message_format: Type[MessageFormat],
        message: bytes,
    ) -> None:
        broker = RedisBroker(apply_types=False, message_format=JSONMessageFormat)

        @broker.subscriber(queue, message_format=message_format)
        async def handler(msg):
            event.set()
            mock(msg)

        async with broker:
            await broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(broker._connection.publish(queue, message)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )
        mock.assert_called_once_with(b"hello")

    @pytest.mark.parametrize(
        ("message_format", "message"),
        [
            (JSONMessageFormat, b'{"data":"hello"}'),
            (
                BinaryMessageFormatV1,
                b"\x89BIN\x0d\x0a\x1a\x0a\x00\x01\x00\00\x00\x12\x00\x00\x00\x14\x00\x00hello",
            ),
        ],
    )
    async def test_publish_in_different_formats(
        self,
        queue: str,
        event: asyncio.Event,
        mock: MagicMock,
        message_format: Type[MessageFormat],
        message: bytes,
    ) -> None:
        broker = RedisBroker(apply_types=False, message_format=message_format)

        @broker.subscriber(queue, message_format=message_format)
        @broker.publisher(queue + "resp")
        async def resp(msg):
            return msg

        @broker.subscriber(queue + "resp", message_format=message_format)
        async def handler(msg):
            mock(msg)
            event.set()

        async with broker:
            await broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(broker._connection.publish(queue, message)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )
        mock.assert_called_once_with(b"hello")

    async def test_publisher_format_overrides_broker(
        self,
        queue: str,
        event: asyncio.Event,
        mock: MagicMock,
    ) -> None:
        broker = RedisBroker(
            apply_types=False, message_format=BinaryMessageFormatV1
        )  # will be ignored

        with pytest.warns(  # noqa: PT031
            DeprecationWarning, match="JSONMessageFormat has been deprecated"
        ):

            @broker.subscriber(queue, message_format=JSONMessageFormat)
            async def resp(msg):
                mock(msg)
                event.set()

            publisher = broker.publisher(queue, message_format=JSONMessageFormat)

        async with broker:
            await broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(publisher.publish("Hi!")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )
        mock.assert_called_once_with("Hi!")

    async def test_parse_json_with_binary_format(
        self, queue: str, event: asyncio.Event, mock: MagicMock
    ) -> None:
        with pytest.warns(
            DeprecationWarning, match="JSONMessageFormat has been deprecated"
        ):
            broker = RedisBroker(apply_types=False, message_format=JSONMessageFormat)

        @broker.subscriber(queue, message_format=BinaryMessageFormatV1)
        async def resp(msg):
            mock(msg)
            event.set()

        async with broker:
            await broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(
                        broker._connection.publish(queue, "hello world")
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )
        mock.assert_called_once_with(b"hello world")


@pytest.mark.asyncio
class TestTestBrokerFormats:
    @pytest.mark.parametrize(("msg_format"), [JSONMessageFormat, BinaryMessageFormatV1])
    async def test_formats(self, queue: str, msg_format: Type["MessageFormat"]) -> None:
        broker = RedisBroker(apply_types=False, message_format=msg_format)

        @broker.subscriber(queue, message_format=msg_format)
        async def handler(msg): ...

        async with TestRedisBroker(broker) as br:
            await br.publish("hello", queue)
            handler.mock.assert_called_once_with("hello")

    async def test_parse_json_with_binary_format(self, queue: str) -> None:
        with pytest.warns(
            DeprecationWarning, match="JSONMessageFormat has been deprecated"
        ):
            broker = RedisBroker(apply_types=False, message_format=JSONMessageFormat)

        @broker.subscriber(queue, message_format=BinaryMessageFormatV1)
        async def handler(msg): ...

        async with TestRedisBroker(broker) as br:
            await br.publish("hello", queue)
            handler.mock.assert_called_once_with("hello")
