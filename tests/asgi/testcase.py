from typing import Any
from unittest.mock import AsyncMock

import pytest
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from faststream.asgi import AsgiFastStream, AsgiResponse, get, make_ping_asgi
from faststream.asyncapi.generate import get_app_schema


class AsgiTestcase:
    def get_broker(self) -> Any:
        raise NotImplementedError()

    def get_test_broker(self, broker) -> Any:
        raise NotImplementedError()

    @pytest.mark.asyncio
    async def test_not_found(self):
        broker = self.get_broker()
        app = AsgiFastStream(broker)

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.get("/")
                assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_ws_not_found(self):
        broker = self.get_broker()

        app = AsgiFastStream(broker)

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                with pytest.raises(WebSocketDisconnect):
                    with client.websocket_connect("/ws"):  # raises error
                        pass

    @pytest.mark.asyncio
    async def test_asgi_ping_unhealthy(self):
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

    @pytest.mark.asyncio
    async def test_asgi_ping_healthy(self):
        broker = self.get_broker()

        app = AsgiFastStream(
            broker,
            asgi_routes=[("/health", make_ping_asgi(broker, timeout=5.0))],
        )

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.get("/health")
                assert response.status_code == 204

    @pytest.mark.asyncio
    async def test_asyncapi_asgi(self):
        broker = self.get_broker()

        app = AsgiFastStream(broker, asyncapi_path="/docs")

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.get("/docs")
                assert response.status_code == 200
                assert response.text

    @pytest.mark.asyncio
    async def test_get_decorator(self):
        @get
        async def some_handler(scope):
            return AsgiResponse(body=b"test", status_code=200)

        broker = self.get_broker()
        app = AsgiFastStream(broker, asgi_routes=[("/test", some_handler)])

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.get("/test")
                assert response.status_code == 200
                assert response.text == "test"

    @pytest.mark.asyncio
    async def test_get_decorator_with_include_in_schema(self):
        @get(include_in_schema=True)
        async def some_handler(scope):
            return AsgiResponse(body=b"test", status_code=200)

        broker = self.get_broker()
        app = AsgiFastStream(broker, asgi_routes=[("/test", some_handler)])

        assert app.routes[0][1].include_in_schema is True

    def test_asyncapi_generate(self):
        broker = self.get_broker()

        @get(include_in_schema=True)
        async def liveness_ping(scope):
            """Liveness ping."""
            return AsgiResponse(b"", status_code=200)

        routes = [
            ("/liveness", liveness_ping),
            ("/readiness", make_ping_asgi(broker, timeout=5.0, include_in_schema=True)),
        ]

        schema = get_app_schema(
            AsgiFastStream(broker, asgi_routes=routes)
        ).to_jsonable()

        assert schema["routes"][0] == {
            "path": "/liveness",
            "methods": ["GET", "HEAD"],
            "description": "Liveness ping.",
        }

        assert schema["routes"][1] == {"path": "/readiness", "methods": ["GET", "HEAD"]}
