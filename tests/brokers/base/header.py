import asyncio
from itertools import starmap
from typing import Any
from unittest.mock import MagicMock

import pytest

from faststream import Context, Response
from tests.brokers.base.basic import BaseTestcaseConfig

INPUT_HEADERS: dict[str, str | bytes | None] = {
    "text": "plain",
    "utf8": "привет".encode(),
    "binary": b"\xc3\x28",
    "empty": b"",
    "nullable": None,
}
DECODED_HEADERS = {
    "text": "plain",
    "utf8": "привет",
    "binary": "\ufffd(",
    "empty": "",
}
RAW_HEADERS = {
    "text": b"plain",
    "utf8": "привет".encode(),
    "binary": b"\xc3\x28",
    "empty": b"",
    "nullable": None,
}
HeaderSnapshot = tuple[dict[str, str], dict[str, bytes | None]]


def _snapshot(
    headers: dict[str, str],
    raw_headers: dict[str, bytes | None],
) -> HeaderSnapshot:
    return (
        {key: value for key, value in headers.items() if key in INPUT_HEADERS},
        {key: value for key, value in raw_headers.items() if key in INPUT_HEADERS},
    )


@pytest.mark.asyncio()
class BinaryHeadersPublishTestcase(BaseTestcaseConfig[Any]):
    async def test_publish_binary_headers(self, queue: str) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2214."""
        broker = self.get_broker(apply_types=True)
        args, kwargs = self.get_subscriber_params(queue)
        received: asyncio.Queue[HeaderSnapshot] = asyncio.Queue()

        @broker.subscriber(*args, **kwargs)
        async def handle(message: Any = Context("message")) -> None:
            await received.put(_snapshot(message.headers, message.raw_headers))

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("body", queue, headers=INPUT_HEADERS)
            result = await asyncio.wait_for(received.get(), timeout=self.timeout)

        assert result == (DECODED_HEADERS, RAW_HEADERS)


class BinaryHeadersTestcase(BinaryHeadersPublishTestcase):
    async def test_publish_batch_binary_headers(
        self, queue: str, mock: MagicMock
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2214."""
        broker = self.get_broker(apply_types=True)
        args, kwargs = self.get_subscriber_params(queue, batch=True)

        @broker.subscriber(*args, **kwargs)
        async def handle(message: Any = Context("message")) -> None:
            mock((
                _snapshot(message.headers, message.raw_headers),
                list(
                    starmap(
                        _snapshot,
                        zip(
                            message.batch_headers, message.batch_raw_headers, strict=True
                        ),
                    )
                ),
            ))

        async with self.patch_broker(broker) as br:
            await br.publish_batch("one", "two", topic=queue, headers=INPUT_HEADERS)

        expected = (DECODED_HEADERS, RAW_HEADERS)
        mock.assert_called_once_with((expected, [expected, expected]))

    async def test_request_forwards_binary_response_headers(self, queue: str) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2214."""
        broker = self.get_broker(apply_types=True)
        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handle(message: Any = Context("message")) -> Response:
            return Response(await message.decode(), headers=message.raw_headers)

        async with self.patch_broker(broker) as br:
            response = await br.request("body", queue, headers=INPUT_HEADERS)

        assert (
            await response.decode(),
            _snapshot(response.headers, response.raw_headers),
        ) == ("body", (DECODED_HEADERS, RAW_HEADERS))
