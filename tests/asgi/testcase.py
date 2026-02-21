from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fast_depends import Depends
from starlette.applications import Starlette
from starlette.routing import Mount
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from faststream._internal.context import Context
from faststream.annotations import FastStream, Logger
from faststream.asgi import (
    AsgiFastStream,
    AsgiResponse,
    AsyncAPIRoute,
    Request,
    get,
    make_asyncapi_asgi,
    make_ping_asgi,
    post,
)
from faststream.asgi.params import Header, Query
from faststream.asgi.types import ASGIApp, Scope
from faststream.specification import AsyncAPI


class AsgiTestcase:
    def get_broker(self) -> Any:
        raise NotImplementedError

    def get_test_broker(self, broker: Any) -> Any:
        raise NotImplementedError

    def get_try_it_out_channel_name(self) -> str:
        """Channel/topic/queue/subject name for try-it-out tests. Override per broker if needed."""
        return "test_channel"

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
            specification=AsyncAPI(),
            asyncapi_path="/docs",
        )

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.get("/docs")
                assert response.status_code == 200, response
                assert response.text

    @pytest.mark.asyncio()
    async def test_asyncapi_asgi_if_broker_set_by_method(self) -> None:
        broker = self.get_broker()

        app = AsgiFastStream(
            specification=AsyncAPI(),
            asyncapi_path="/docs",
        )

        app.set_broker(broker)

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.get("/docs")
                assert response.status_code == 200, response
                assert response.text

    @pytest.mark.asyncio()
    @pytest.mark.parametrize(
        ("decorator", "client_method"),
        (
            pytest.param(get, "get", id="get"),
            pytest.param(post, "post", id="post"),
        ),
    )
    async def test_decorators(
        self, decorator: Callable[..., ASGIApp], client_method: str
    ) -> None:
        @decorator
        async def some_handler(scope: Scope) -> AsgiResponse:
            return AsgiResponse(body=b"test", status_code=200)

        broker = self.get_broker()
        app = AsgiFastStream(broker, asgi_routes=[("/test", some_handler)])

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = getattr(client, client_method)("/test")
                assert response.status_code == 200
                assert response.text == "test"

    @pytest.mark.asyncio()
    @pytest.mark.parametrize(
        ("decorator", "client_method"),
        (
            pytest.param(get, "get", id="get"),
            pytest.param(post, "post", id="post"),
        ),
    )
    async def test_context_injected(
        self, decorator: Callable[..., ASGIApp], client_method: str
    ) -> None:
        @decorator
        async def some_handler(
            request: Request, logger: Logger, app: FastStream
        ) -> AsgiResponse:
            return AsgiResponse(
                body=f"{request.__class__.__name__} {logger.__class__.__name__} {app.__class__.__name__}".encode(),
                status_code=200,
            )

        broker = self.get_broker()
        app = AsgiFastStream(broker, asgi_routes=[("/test", some_handler)])

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = getattr(client, client_method)("/test")
                assert response.status_code == 200
                assert response.text == "AsgiRequest Logger AsgiFastStream"

    @pytest.mark.asyncio()
    @pytest.mark.parametrize(
        ("decorator", "client_method"),
        (
            pytest.param(get, "get", id="get"),
            pytest.param(post, "post", id="post"),
        ),
    )
    async def test_fast_depends_injected(
        self, decorator: Callable[..., ASGIApp], client_method: str
    ) -> None:
        def get_string() -> str:
            return "test"

        @decorator
        async def some_handler(string=Depends(get_string)) -> AsgiResponse:  # noqa: B008
            return AsgiResponse(body=string.encode(), status_code=200)

        broker = self.get_broker()
        app = AsgiFastStream(broker, asgi_routes=[("/test", some_handler)])

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = getattr(client, client_method)("/test")
                assert response.status_code == 200
                assert response.text == "test"

    @pytest.mark.asyncio()
    @pytest.mark.parametrize(
        "dependency",
        (
            pytest.param(Query(), id="query"),
            pytest.param(Header(), id="header"),
        ),
    )
    async def test_validation_error_handled(self, dependency: Context) -> None:
        @get
        async def some_handler(dep=dependency) -> AsgiResponse:
            return AsgiResponse(status_code=200)

        broker = self.get_broker()
        app = AsgiFastStream(broker, asgi_routes=[("/test", some_handler)])

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.get("/test")
                assert response.status_code == 422
                assert response.text == "Validation error"

    def test_asyncapi_pure_asgi(self) -> None:
        broker = self.get_broker()

        app = Starlette(routes=[Mount("/", make_asyncapi_asgi(AsyncAPI(broker)))])

        with TestClient(app) as client:
            response = client.get("/")
            assert response.status_code == 200
            assert response.text.strip().startswith("<!DOCTYPE html>")

    @pytest.mark.asyncio()
    async def test_try_it_out_endpoint_registered(self) -> None:
        """AsgiFastStream with try_it_out=True registers POST /path/try endpoint."""
        broker = self.get_broker()
        channel = self.get_try_it_out_channel_name()

        @broker.subscriber(channel)
        async def handler(msg: dict[str, Any]) -> None:
            pass

        route = AsyncAPIRoute("/docs")
        app = AsgiFastStream(broker, asyncapi_path=route)

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                assert client.get("/docs").status_code == 200
                # Endpoint exists: returns 400 (validation), not 404
                response = client.post("/docs/try", json={})
                assert response.status_code == 400
                assert response.status_code != 404

    @pytest.mark.asyncio()
    async def test_try_it_out_endpoint_post_success(self) -> None:
        """POST to try-it-out with valid payload returns 200 and mode=test."""
        broker = self.get_broker()
        channel = self.get_try_it_out_channel_name()

        @broker.subscriber(channel)
        async def handler(msg: dict[str, Any]) -> None:
            pass

        route = AsyncAPIRoute("/asyncapi", try_it_out=True)
        app = AsgiFastStream(broker, asyncapi_path=route)

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.post(
                    "/asyncapi/try",
                    json={
                        "channelName": channel,
                        "message": {"data": "hello"},
                        "options": {"sendToRealBroker": False},
                    },
                )
                assert response.status_code == 200
                assert response.json() == {"status": "ok", "mode": "test"}

    @pytest.mark.asyncio()
    async def test_try_it_out_message_delivered_to_subscriber(self) -> None:
        """POST with sendToRealBroker=False routes message through TestBroker to subscriber."""
        broker = self.get_broker()
        channel = self.get_try_it_out_channel_name()
        received: list[Any] = []

        @broker.subscriber(channel)
        async def handler(msg: Any) -> None:
            received.append(msg)

        route = AsyncAPIRoute("/asyncapi", try_it_out=True)
        app = AsgiFastStream(broker, asyncapi_path=route)

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.post(
                    "/asyncapi/try",
                    json={
                        "channelName": channel,
                        "message": {"text": "hello"},
                        "options": {"sendToRealBroker": False},
                    },
                )
        assert response.status_code == 200
        assert len(received) == 1

    @pytest.mark.asyncio()
    async def test_try_it_out_missing_channel_returns_400(self) -> None:
        """POST without channelName returns 400 with descriptive error."""
        broker = self.get_broker()
        route = AsyncAPIRoute("/docs", try_it_out=True)
        app = AsgiFastStream(broker, asyncapi_path=route)

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.post("/docs/try", json={"message": {}})
                assert response.status_code == 400
                assert "channelname" in response.json().get("error", "").lower()

    @pytest.mark.asyncio()
    async def test_try_it_out_path_follows_asyncapi_path(self) -> None:
        """Try-it-out endpoint is registered at {asyncapi_path}/try."""
        broker = self.get_broker()
        channel = self.get_try_it_out_channel_name()

        @broker.subscriber(channel)
        async def handler(msg: dict[str, Any]) -> None:
            pass

        route = AsyncAPIRoute("/custom/docs", try_it_out=True)
        app = AsgiFastStream(broker, asyncapi_path=route)

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                assert client.get("/custom/docs").status_code == 200
                response = client.post(
                    "/custom/docs/try",
                    json={
                        "channelName": channel,
                        "message": {},
                        "options": {"sendToRealBroker": False},
                    },
                )
                assert response.status_code == 200
                assert response.json()["status"] == "ok"

    @pytest.mark.asyncio()
    async def test_try_it_out_spec_endpoint_base_overrides_route_default(self) -> None:
        """AsyncAPI spec's try_it_out_endpoint_base overrides the route's derived default."""
        broker = self.get_broker()
        channel = self.get_try_it_out_channel_name()

        @broker.subscriber(channel)
        async def handler(msg: dict[str, Any]) -> None:
            pass

        spec = AsyncAPI(broker, try_it_out_endpoint_base="https://api.example.com/try")
        route = AsyncAPIRoute("/docs", try_it_out=True)
        app = AsgiFastStream(broker, specification=spec, asyncapi_path=route)

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.get("/docs")
                assert response.status_code == 200
                assert "https://api.example.com/try" in response.text
