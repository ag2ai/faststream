import json
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import parse_qs

from faststream._internal.constants import EMPTY

if TYPE_CHECKING:
    from .types import Receive, Scope, Send


class ClientDisconnectError(Exception): ...


class AsgiRequest:
    def __init__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        self._scope = scope
        self._receive = receive
        self._send = send
        self._stream_consumed = False
        self._body: bytes | None = None
        self._json: Any = EMPTY
        self._query_params: dict[str, list[str]] | None = None
        self._headers: dict[str, str] | None = None

    async def stream(self) -> AsyncGenerator[bytes, None]:
        if self._body is not None:
            yield self._body
            yield b""
            return
        if self._stream_consumed:
            msg = "Stream consumed"
            raise RuntimeError(msg)
        while not self._stream_consumed:
            message = await self._receive()
            if message["type"] == "http.request":
                body = message.get("body", b"")
                if not message.get("more_body", False):
                    self._stream_consumed = True
                if body:
                    yield body
            elif message["type"] == "http.disconnect":  # pragma: no branch
                self._is_disconnected = True
                raise ClientDisconnectError
        yield b""

    async def body(self) -> bytes:
        if self._body is None:
            chunks: list[bytes] = [chunk async for chunk in self.stream()]
            self._body = b"".join(chunks)
        return self._body

    async def json(self) -> Any:
        if self._json is EMPTY:
            body = await self.body()
            self._json = json.loads(body)
        return self._json

    @property
    def query_params(self) -> dict[str, list[str]]:
        if self._query_params is None:
            query_string = self._scope.get("query_string", b"")
            self._query_params = {
                key.decode("latin-1"): [v.decode("latin-1") for v in value]
                for key, value in parse_qs(query_string).items()
            }
        return self._query_params

    @property
    def headers(self) -> dict[str, str]:
        if self._headers is None:
            self._headers = {
                key.lower().decode("latin-1"): value.decode("latin-1")
                for key, value in list(self._scope["headers"])
            }
        return self._headers

    @property
    def method(self) -> str:
        return cast("str", self._scope["method"])
