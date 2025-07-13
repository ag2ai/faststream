from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock

import pytest
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from faststream.asgi import AsgiFastStream, AsgiResponse, get, make_ping_asgi


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
    async def test_lifespan_startup_failure(self):
        @asynccontextmanager
        async def lifespan():
            raise RuntimeError("Lifespan failure")
            yield

        broker = self.get_broker()
        app = AsgiFastStream(broker, lifespan=lifespan)

        async with self.get_test_broker(broker):
            with pytest.raises(RuntimeError, match="Lifespan failure"):
                with TestClient(app):
                    pass

    @pytest.mark.asyncio
    async def test_lifespan_shutdown_failure(self):
        @asynccontextmanager
        async def lifespan():
            yield
            raise RuntimeError("Lifespan failure")

        broker = self.get_broker()
        app = AsgiFastStream(broker, lifespan=lifespan)

        async with self.get_test_broker(broker):
            with pytest.raises(RuntimeError, match="Lifespan failure"):
                with TestClient(app):
                    pass

    @pytest.mark.asyncio
    async def test_on_startup_failure(self):
        broker = self.get_broker()
        app = AsgiFastStream(broker)

        @app.on_startup
        async def on_startup():
            raise RuntimeError("Startup failure")

        async with self.get_test_broker(broker):
            with pytest.raises(RuntimeError, match="Startup failure"):
                with TestClient(app):
                    pass

    @pytest.mark.asyncio
    async def test_on_shutdown_failure(self):
        broker = self.get_broker()
        app = AsgiFastStream(broker)

        @app.on_shutdown
        async def on_shutdown():
            raise RuntimeError("Shutdown failure")

        async with self.get_test_broker(broker):
            with pytest.raises(RuntimeError, match="Shutdown failure"):
                with TestClient(app):
                    pass
