from typing import Any
from unittest.mock import AsyncMock

import pytest
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from faststream.asgi import (
    AsgiFastStream,
    AsgiResponse,
    get,
    make_asyncapi_asgi,
    make_ping_asgi,
)
from faststream.asgi.types import Scope
from faststream.specification import AsyncAPI


class AsgiTestcase:
    def get_broker(self) -> Any:
        raise NotImplementedError

    def get_test_broker(self, broker: Any) -> Any:
        raise NotImplementedError

    @pytest.mark.asyncio()
    async def test_not_found(self) -> None:
        broker = self.get_broker()
        app = AsgiFastStream(broker)

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.get("/")
                assert response.status_code == 404

    @pytest.mark.asyncio()
    async def test_ws_not_found(self) -> None:
        broker = self.get_broker()

        app = AsgiFastStream(broker)

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                with pytest.raises(WebSocketDisconnect):
                    with client.websocket_connect("/ws"):  # raises error
                        pass

    @pytest.mark.asyncio()
    async def test_asgi_ping_healthy(self) -> None:
        broker = self.get_broker()

        app = AsgiFastStream(
            broker,
            asgi_routes=[("/health", make_ping_asgi(broker, timeout=5.0))],
        )

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.get("/health")
                assert response.status_code == 204

    @pytest.mark.asyncio()
    async def test_asgi_ping_unhealthy(self) -> None:
        broker = self.get_broker()

        app = AsgiFastStream(
            broker,
            asgi_routes=[
                ("/health", make_ping_asgi(broker, timeout=5.0)),
            ],
        )
        async with self.get_test_broker(broker) as br:
            br.ping = AsyncMock()
            br.ping.return_value = False

            with TestClient(app) as client:
                response = client.get("/health")
                assert response.status_code == 500

    @pytest.mark.asyncio()
    async def test_asyncapi_asgi(self) -> None:
        broker = self.get_broker()

        app = AsgiFastStream(
            broker,
            asgi_routes=[("/docs", make_asyncapi_asgi(AsyncAPI(broker)))],
        )

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.get("/docs")
                assert response.status_code == 200
                assert response.text

    @pytest.mark.asyncio()
    async def test_get_decorator(self) -> None:
        @get
        async def some_handler(scope: Scope) -> AsgiResponse:
            return AsgiResponse(body=b"test", status_code=200)

        broker = self.get_broker()
        app = AsgiFastStream(broker, asgi_routes=[("/test", some_handler)])

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.get("/test")
                assert response.status_code == 200
                assert response.text == "test"
