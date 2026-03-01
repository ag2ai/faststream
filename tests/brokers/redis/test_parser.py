import asyncio
import json
from typing import Any
from unittest.mock import MagicMock

import anyio
import pytest

from faststream._internal._compat import json_dumps
from faststream.redis import RedisBroker, TestRedisBroker
from faststream.redis.parser import (
    BinaryMessageFormatV1,
    JSONMessageFormat,
    MessageFormat,
)
from tests.brokers.base.parser import CustomParserTestcase

from .basic import RedisTestcaseConfig


@pytest.mark.connected()
@pytest.mark.redis()
class TestCustomParser(RedisTestcaseConfig, CustomParserTestcase):
    async def test_request_respect_decoder(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        async def custom_decoder(msg, original):
            mock()
            return await original(msg)

        broker = self.get_broker(decoder=custom_decoder)

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg):
            return msg

        async with self.patch_broker(broker) as br:
            await br.start()

            with anyio.fail_after(self.timeout):
                msg = await br.request("Hi!", queue)

        assert mock.call_count == 1
        await msg.decode()
        assert mock.call_count == 2


@pytest.mark.parametrize(
    ("input", "should_be"),
    (
        pytest.param("", b"", id="empty"),
        pytest.param("plain text", b"plain text", id="plain text"),
        pytest.param(
            {"id": "12345678" * 4, "date": "2021-01-01T00:00:00Z"},
            json_dumps({"id": "12345678" * 4, "date": "2021-01-01T00:00:00Z"}),
            id="complex json",
        ),
        pytest.param(
            b"\x82\xa2id\xd9",
            b"\x82\xa2id\xd9",
            id="UTF-8 incompatible bytes",
        ),
    ),
)
@pytest.mark.redis()
def test_binary_message_encode_parse(input: Any, should_be: bytes) -> None:
    raw_message = BinaryMessageFormatV1.encode(
        message=input, reply_to=None, headers=None, correlation_id="id"
    )
    parsed, _ = BinaryMessageFormatV1.parse(raw_message)
    assert parsed == should_be


@pytest.mark.parametrize(
    ("input", "should_be"),
    (
        pytest.param("", b"", id="empty"),
        pytest.param("plain text", b"plain text", id="plain text"),
        pytest.param(
            {"id": "12345678" * 4, "date": "2021-01-01T00:00:00Z"},
            json_dumps({"id": "12345678" * 4, "date": "2021-01-01T00:00:00Z"}),
            id="complex json",
        ),
    ),
)
@pytest.mark.redis()
def test_json_message_encode_parse(input: Any, should_be: bytes) -> None:
    raw_message = JSONMessageFormat.encode(
        message=input, reply_to=None, headers=None, correlation_id="id"
    )
    parsed, _ = JSONMessageFormat.parse(raw_message)
    assert parsed == should_be


@pytest.mark.redis()
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


@pytest.mark.redis()
@pytest.mark.connected()
@pytest.mark.asyncio()
class TestFormats:
    @pytest.mark.parametrize(
        ("message_format", "message"),
        (
            pytest.param(
                JSONMessageFormat,
                b'{"data": "hello"}',
                id="json",
                marks=pytest.mark.filterwarnings(
                    "ignore:JSONMessageFormat has been deprecated"
                ),
            ),
            pytest.param(
                BinaryMessageFormatV1,
                b"\x89BIN\x0d\x0a\x1a\x0a\x00\x01\x00\00\x00\x12\x00\x00\x00\x14\x00\x00hello",
                id="binary",
            ),
        ),
    )
    async def test_consume_in_different_formats(
        self,
        queue: str,
        event: asyncio.Event,
        mock: MagicMock,
        message_format: type[MessageFormat],
        message: bytes,
    ) -> None:
        broker = RedisBroker(apply_types=False)

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
        (
            pytest.param(
                JSONMessageFormat,
                b'{"data": "hello"}',
                id="json",
                marks=pytest.mark.filterwarnings(
                    "ignore:JSONMessageFormat has been deprecated"
                ),
            ),
            pytest.param(
                BinaryMessageFormatV1,
                b"\x89BIN\x0d\x0a\x1a\x0a\x00\x01\x00\00\x00\x12\x00\x00\x00\x14\x00\x00hello",
                id="binary",
            ),
        ),
    )
    async def test_publish_in_different_formats(
        self,
        queue: str,
        event: asyncio.Event,
        mock: MagicMock,
        message_format: type[MessageFormat],
        message: bytes,
    ) -> None:
        broker = RedisBroker(apply_types=False)

        @broker.subscriber(queue, message_format=message_format)
        @broker.publisher(queue + "resp", message_format=message_format)
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

    @pytest.mark.filterwarnings("ignore:JSONMessageFormat has been deprecated")
    async def test_publisher_format_overrides_broker(
        self,
        queue: str,
        event: asyncio.Event,
        mock: MagicMock,
    ) -> None:
        broker = RedisBroker(
            apply_types=False,
            message_format=BinaryMessageFormatV1,  # will be ignored
        )

        @broker.subscriber(queue, message_format=JSONMessageFormat)
        async def resp(msg) -> None:
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

    @pytest.mark.filterwarnings("ignore:JSONMessageFormat has been deprecated")
    async def test_parse_json_with_binary_format(
        self, queue: str, event: asyncio.Event, mock: MagicMock
    ) -> None:
        broker = RedisBroker(apply_types=False, message_format=JSONMessageFormat)

        @broker.subscriber(queue, message_format=BinaryMessageFormatV1)
        async def resp(msg):
            mock(msg)
            event.set()

        async with broker:
            await broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(broker._connection.publish(queue, "hello world")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )
        mock.assert_called_once_with(b"hello world")

    @pytest.mark.filterwarnings("ignore:JSONMessageFormat has been deprecated")
    async def test_parse_response_with_publisher_format(self, queue: str) -> None:
        broker = RedisBroker(
            apply_types=False,
            # message_format will be ignored
            message_format=None,  # type: ignore[arg-type]
        )

        @broker.subscriber(queue, message_format=JSONMessageFormat)
        async def resp(msg):
            return "Response"

        publisher = broker.publisher(queue, message_format=JSONMessageFormat)

        async with broker:
            await broker.start()

            response = await publisher.request("Hi!")
            assert response.body == b"Response"


@pytest.mark.asyncio()
@pytest.mark.redis()
class TestTestBrokerFormats:
    @pytest.mark.parametrize(
        ("msg_format"),
        (
            pytest.param(
                JSONMessageFormat,
                id="json",
                marks=pytest.mark.filterwarnings(
                    "ignore:JSONMessageFormat has been deprecated"
                ),
            ),
            pytest.param(BinaryMessageFormatV1, id="binary"),
        ),
    )
    async def test_formats(self, queue: str, msg_format: type["MessageFormat"]) -> None:
        broker = RedisBroker(apply_types=False, message_format=msg_format)

        @broker.subscriber(queue, message_format=msg_format)
        async def handler(msg): ...

        async with TestRedisBroker(broker) as br:
            await br.publish("hello", queue)
            handler.mock.assert_called_once_with("hello")

    @pytest.mark.filterwarnings("ignore:JSONMessageFormat has been deprecated")
    async def test_parse_json_with_binary_format(self, queue: str) -> None:
        broker = RedisBroker(apply_types=False, message_format=JSONMessageFormat)

        @broker.subscriber(queue, message_format=BinaryMessageFormatV1)
        async def handler(msg) -> None: ...

        async with TestRedisBroker(broker) as br:
            await br.publish("hello", queue)
            handler.mock.assert_called_once_with("hello")

    @pytest.mark.connected()
    @pytest.mark.filterwarnings("ignore:JSONMessageFormat has been deprecated")
    async def test_binary_fallback_to_json(
        self, queue: str, mock: MagicMock, event: asyncio.Event
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2552."""
        broker = RedisBroker()

        @broker.subscriber(stream=queue, message_format=BinaryMessageFormatV1)
        async def handler(msg) -> None:
            mock(msg)
            if mock.call_count == 2:
                event.set()

        data = {
            "name": "John",
            "age": "25",
        }

        async with broker:
            await broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(broker._connection.xadd(queue, data)),
                    asyncio.create_task(
                        broker._connection.xadd(queue, {"data": json.dumps(data)})
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        mock.assert_called_with(data)
        assert mock.call_count == 2
