from collections.abc import Generator
from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from redis.exceptions import AuthenticationError

from tests.asyncapi.base.v2_6_0 import get_2_6_0_schema


@contextmanager
def patch_asyncio_open_connection() -> Generator[AsyncMock, None, None]:
    try:
        reader = MagicMock()
        reader.readline = AsyncMock(return_value=b":1\r\n")
        reader.read = AsyncMock(return_value=b"")

        writer = MagicMock()
        writer.drain = AsyncMock()
        writer.wait_closed = AsyncMock()

        open_connection = AsyncMock(return_value=(reader, writer))

        with patch("asyncio.open_connection", new=open_connection):
            yield open_connection
    finally:
        pass


@pytest.mark.asyncio()
@pytest.mark.redis()
async def test_base_security() -> None:
    with patch_asyncio_open_connection() as connection:
        from docs.docs_src.redis.security.basic import broker

        async with broker:
            await broker.ping(0.01)

        assert connection.call_args.kwargs["ssl"]

        schema = get_2_6_0_schema(broker)
        assert schema == {
            "asyncapi": "2.6.0",
            "channels": {},
            "components": {"messages": {}, "schemas": {}, "securitySchemes": {}},
            "defaultContentType": "application/json",
            "info": {"title": "FastStream", "version": "0.1.0"},
            "servers": {
                "development": {
                    "protocol": "redis",
                    "protocolVersion": "custom",
                    "security": [],
                    "url": "redis://localhost:6379",
                },
            },
        }


@pytest.mark.asyncio()
@pytest.mark.redis()
async def test_plaintext_security() -> None:
    with patch_asyncio_open_connection() as connection:
        from docs.docs_src.redis.security.plaintext import broker

        with pytest.raises(AuthenticationError):
            async with broker:
                await broker._connection.ping()

        assert connection.call_args.kwargs["ssl"]

        schema = get_2_6_0_schema(broker)
        assert schema == {
            "asyncapi": "2.6.0",
            "channels": {},
            "components": {
                "messages": {},
                "schemas": {},
                "securitySchemes": {"user-password": {"type": "userPassword"}},
            },
            "defaultContentType": "application/json",
            "info": {"title": "FastStream", "version": "0.1.0"},
            "servers": {
                "development": {
                    "protocol": "redis",
                    "protocolVersion": "custom",
                    "security": [{"user-password": []}],
                    "url": "redis://localhost:6379",
                },
            },
        }
