import math
from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from dirty_equals import Contains, IsFloat, IsList, IsPartialDict, IsStr
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
            assert response.text == Contains("<!DOCTYPE html>")

    # ===== TryItOut tests =====
    @pytest.mark.asyncio()
    async def test_try_it_out_endpoint_post_success(self, queue: str) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue)
        async def handler(msg: dict[str, Any]) -> None:
            pass

        app = AsgiFastStream(broker, asyncapi_path="/asyncapi")

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.post(
                    "/asyncapi/try",
                    json={
                        "channelName": queue,
                        "message": {
                            "operation_id": "op",
                            "operation_type": "subscribe",
                            "message": {"data": "hello"},
                        },
                        "options": {"sendToRealBroker": False},
                    },
                )
                assert response.status_code == 200
                assert response.json() == "ok"

    @pytest.mark.asyncio()
    async def test_try_it_out_message_delivered_to_subscriber(
        self, queue: str, mock: MagicMock
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue)
        async def handler(msg: Any) -> None:
            mock(msg)

        app = AsgiFastStream(
            broker,
            asyncapi_path=AsyncAPIRoute("/asyncapi", try_it_out=True),
        )

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                client.post(
                    "/asyncapi/try",
                    json={
                        "channelName": queue,
                        "message": {
                            "operation_id": "op",
                            "operation_type": "subscribe",
                            "message": {"text": "hello"},
                        },
                        "options": {"sendToRealBroker": False},
                    },
                )

        mock.assert_called_once_with(IsPartialDict(text="hello"))

    @pytest.mark.asyncio()
    async def test_try_it_out_string_payload_delivered(
        self, queue: str, mock: MagicMock
    ) -> None:
        """Plugin wraps primitive payloads in message.message — ensure they arrive correctly."""
        broker = self.get_broker()

        @broker.subscriber(queue)
        async def handler(msg: Any) -> None:
            mock(msg)

        app = AsgiFastStream(broker, asyncapi_path="/asyncapi")

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                client.post(
                    "/asyncapi/try",
                    json={
                        "channelName": queue,
                        "message": {
                            "operation_id": "op",
                            "operation_type": "subscribe",
                            "message": "hello",
                        },
                        "options": {"sendToRealBroker": False},
                    },
                )

        mock.assert_called_once_with("hello")

    @pytest.mark.asyncio()
    async def test_try_it_out_integer_payload_delivered(
        self, queue: str, mock: MagicMock
    ) -> None:
        """Primitive int payload should arrive correctly."""
        broker = self.get_broker()

        @broker.subscriber(queue)
        async def handler(msg: int) -> None:
            mock(msg)

        app = AsgiFastStream(broker, asyncapi_path="/asyncapi")

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                client.post(
                    "/asyncapi/try",
                    json={
                        "channelName": queue,
                        "message": {
                            "operation_id": "op",
                            "operation_type": "subscribe",
                            "message": 42,
                        },
                        "options": {"sendToRealBroker": False},
                    },
                )

        mock.assert_called_once_with(42)

    @pytest.mark.asyncio()
    async def test_try_it_out_float_payload_delivered(
        self, queue: str, mock: MagicMock
    ) -> None:
        """Primitive float payload should arrive correctly."""
        broker = self.get_broker()

        @broker.subscriber(queue)
        async def handler(msg: float) -> None:
            mock(msg)

        app = AsgiFastStream(broker, asyncapi_path="/asyncapi")

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                client.post(
                    "/asyncapi/try",
                    json={
                        "channelName": queue,
                        "message": {
                            "operation_id": "op",
                            "operation_type": "subscribe",
                            "message": math.pi,
                        },
                        "options": {"sendToRealBroker": False},
                    },
                )

        mock.assert_called_once_with(IsFloat(approx=math.pi))

    @pytest.mark.asyncio()
    async def test_try_it_out_boolean_payload_delivered(
        self, queue: str, mock: MagicMock
    ) -> None:
        """Primitive boolean payload should arrive correctly."""
        broker = self.get_broker()

        @broker.subscriber(queue)
        async def handler(msg: bool) -> None:
            mock(msg)

        app = AsgiFastStream(broker, asyncapi_path="/asyncapi")

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                client.post(
                    "/asyncapi/try",
                    json={
                        "channelName": queue,
                        "message": {
                            "operation_id": "op",
                            "operation_type": "subscribe",
                            "message": True,
                        },
                        "options": {"sendToRealBroker": False},
                    },
                )

        mock.assert_called_once_with(True)

    @pytest.mark.asyncio()
    async def test_try_it_out_array_payload_delivered(
        self, queue: str, mock: MagicMock
    ) -> None:
        """Array payload should arrive correctly."""
        broker = self.get_broker()

        @broker.subscriber(queue)
        async def handler(msg: list[Any]) -> None:
            mock(msg)

        app = AsgiFastStream(broker, asyncapi_path="/asyncapi")

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                client.post(
                    "/asyncapi/try",
                    json={
                        "channelName": queue,
                        "message": {
                            "operation_id": "op",
                            "operation_type": "subscribe",
                            "message": ["one", "two", "three"],
                        },
                        "options": {"sendToRealBroker": False},
                    },
                )

        mock.assert_called_once_with(IsList("one", "two", "three"))

    @pytest.mark.asyncio()
    async def test_try_it_out_object_payload_delivered(
        self, queue: str, mock: MagicMock
    ) -> None:
        """Object (dict) payload should arrive correctly."""
        broker = self.get_broker()

        @broker.subscriber(queue)
        async def handler(msg: dict[str, Any]) -> None:
            mock(msg)

        app = AsgiFastStream(broker, asyncapi_path="/asyncapi")

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                client.post(
                    "/asyncapi/try",
                    json={
                        "channelName": queue,
                        "message": {
                            "operation_id": "op",
                            "operation_type": "subscribe",
                            "message": {"field": "value", "count": 42, "mock": True},
                        },
                        "options": {"sendToRealBroker": False},
                    },
                )

        mock.assert_called_once_with(IsPartialDict(field="value", count=42, mock=True))

    @pytest.mark.asyncio()
    async def test_try_it_out_memory_subsricber_returns_result(self, queue: str) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue)
        async def handler(msg: dict[str, Any]) -> dict[str, Any]:
            return {"result": 1}

        app = AsgiFastStream(broker, asyncapi_path="/asyncapi")

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.post(
                    "/asyncapi/try",
                    json={
                        "channelName": queue,
                        "message": {
                            "operation_id": "op",
                            "operation_type": "subscribe",
                            "message": {"data": "hello"},
                        },
                        "options": {"sendToRealBroker": False},
                    },
                )
                assert response.status_code == 200, response.json()
                assert response.json() == IsPartialDict(result=1)

    @pytest.mark.asyncio()
    async def test_try_it_out_disabled(self, queue: str) -> None:
        broker = self.get_broker()

        app = AsgiFastStream(
            broker,
            asyncapi_path=AsyncAPIRoute("/asyncapi", try_it_out=False),
        )

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                r = client.post(
                    "/asyncapi/try",
                    json={
                        "channelName": queue,
                        "message": {
                            "operation_id": "op",
                            "operation_type": "subscribe",
                            "message": {"text": "hello"},
                        },
                        "options": {"sendToRealBroker": False},
                    },
                )
                assert r.status_code == 404

    @pytest.mark.asyncio()
    async def test_try_it_out_missing_channel_returns_400(self) -> None:
        broker = self.get_broker()

        app = AsgiFastStream(broker, asyncapi_path="/docs")

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.post("/docs/try", json={"message": {}})
                assert response.status_code == 400
                assert response.json() == IsPartialDict(details="Missing channelName")

    @pytest.mark.asyncio()
    async def test_try_it_out_channel_not_found(self, queue: str) -> None:
        broker = self.get_broker()

        app = AsgiFastStream(broker, asyncapi_path="/docs")

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.post(
                    "/docs/try",
                    json={
                        "channelName": queue,
                        "message": {
                            "operation_id": "op",
                            "operation_type": "subscribe",
                            "message": {"text": "hello"},
                        },
                        "options": {"sendToRealBroker": False},
                    },
                )
                assert response.status_code == 404, response.status_code
                assert response.json() == IsPartialDict(
                    details=IsStr(regex=r".+ destination not found\.")
                )

    @pytest.mark.asyncio()
    async def test_try_it_out_path_follows_asyncapi_path(self, queue: str) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue)
        async def handler(msg: dict[str, Any]) -> None:
            pass

        app = AsgiFastStream(broker, asyncapi_path="/custom/docs")

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.post(
                    "/custom/docs/try",
                    json={
                        "channelName": queue,
                        "message": {
                            "operation_id": "op",
                            "operation_type": "subscribe",
                            "message": {},
                        },
                        "options": {"sendToRealBroker": False},
                    },
                )

                assert response.status_code == 200
                assert response.json() == "ok"

    @pytest.mark.asyncio()
    async def test_try_it_out_spec_endpoint_base_overrides_route_default(self) -> None:
        broker = self.get_broker()

        app = AsgiFastStream(
            broker,
            asyncapi_path=AsyncAPIRoute(
                "/docs",
                try_it_out_url="https://api.example.com/try",
            ),
        )

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.get("/docs")
                assert response.status_code == 200
                assert response.text == Contains("https://api.example.com/try")
