from typing import Any

from redis.asyncio.client import Redis
from redis.asyncio.connection import ConnectionPool

from faststream.__about__ import __version__
from faststream.exceptions import IncorrectState


class ConnectionState:
    def __init__(self, options: dict[str, Any] | None = None) -> None:
        self._options = options or {}

        self._connected = False
        self._client: Redis[bytes] | None = None

    @property
    def client(self) -> "Redis[bytes]":
        if not self._client:
            msg = "Connection is not available yet. Please, connect the broker first."
            raise IncorrectState(msg)

        return self._client

    def __bool__(self) -> bool:
        return self._connected

    async def connect(self) -> "Redis[bytes]":
        pool = ConnectionPool(
            **self._options,
            lib_name="faststream",
            lib_version=__version__,
        )
        client: Redis[bytes] = Redis.from_pool(pool)  # type: ignore[attr-defined]

        self._client = client
        self._connected = True

        return client

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()  # type: ignore[attr-defined]

        self._client = None
        self._connected = False
