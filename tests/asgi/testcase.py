import asyncio
import math
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from dirty_equals import Contains, IsFloat, IsList, IsPartialDict, IsStr
from freezegun import freeze_time
from starlette.applications import Starlette
from starlette.routing import Mount
from starlette.testclient import TestClient

from faststream.asgi import (
    AsgiFastStream,
    AsyncAPIRoute,
    make_asyncapi_asgi,
    make_ping_asgi,
)
from faststream.specification import AsyncAPI


class AsgiTestcase:
    def get_broker(self) -> Any:
        raise NotImplementedError

    def get_test_broker(self, broker: Any) -> Any:
        raise NotImplementedError

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
    async def test_asyncapi_asgi_if_broker_set_by_method(self) -> None:
        broker = self.get_broker()

        app = AsgiFastStream(
            specification=AsyncAPI(),
            asyncapi_path="/docs",
        )

        app.add_broker(broker)

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.get("/docs")
                assert response.status_code == 200, response
                assert response.text

    def test_asyncapi_pure_asgi(self) -> None:
        broker = self.get_broker()

        app = Starlette(routes=[Mount("/", make_asyncapi_asgi(AsyncAPI(broker)))])

        with TestClient(app) as client:
            response = client.get("/")
            assert response.status_code == 200
            assert response.text == Contains("<!DOCTYPE html>")

    # ===== TryItOut tests =====
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
            asyncapi_path=AsyncAPIRoute("/asyncapi"),
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
            asyncapi_path=AsyncAPIRoute("/asyncapi", try_it_out_path=None),
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
    @freeze_time(auto_tick_seconds=5)
    async def test_try_it_out_subscriber_completes_within_timeout(
        self, queue: str
    ) -> None:
        """Subscriber that finishes before the configured timeout should return a 200 with its result."""
        broker = self.get_broker()

        @broker.subscriber(queue)
        async def handler(msg: Any) -> dict[str, Any]:
            await asyncio.sleep(5)
            return {"result": "done"}

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
        assert response.json() == IsPartialDict(result="done")

    @pytest.mark.asyncio()
    @freeze_time(auto_tick_seconds=30)
    async def test_try_it_out_subscriber_exceeds_timeout_returns_500(
        self, queue: str
    ) -> None:
        """Subscriber that runs longer than the configured timeout should produce a 500 carrying the timeout exception."""
        broker = self.get_broker()

        @broker.subscriber(queue)
        async def handler(msg: Any) -> None:
            await asyncio.sleep(30)

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

        assert response.status_code == 500
        assert response.json() == IsPartialDict(details=Contains("TimeoutError"))

    @pytest.mark.asyncio()
    async def test_try_it_out_spec_endpoint_base_overrides_route_default(self) -> None:
        broker = self.get_broker()

        app = AsgiFastStream(
            broker,
            asyncapi_path=AsyncAPIRoute(
                "/docs",
                try_it_out_path="https://api.example.com/try",
            ),
        )

        async with self.get_test_broker(broker):
            with TestClient(app) as client:
                response = client.get("/docs")
                assert response.status_code == 200
                assert response.text == Contains("https://api.example.com/try")
