"""ASGI-specific logic that does not depend on a concrete broker.

These tests exercise routing, the AsyncAPI docs/JSON endpoints, the ASGI
handler decorators and dependency injection directly — no broker is required,
so they run once instead of being inherited by every broker test case. Broker
behaviour (message delivery, healthchecks, try-it-out dispatch) lives in
``AsgiTestcase`` and is still parametrized per broker.
"""

from collections.abc import Callable

import pytest
from fast_depends import Depends
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
    post,
)
from faststream.asgi.params import Header, Query
from faststream.asgi.types import ASGIApp, Scope
from faststream.specification import AsyncAPI


def test_not_found() -> None:
    client = TestClient(AsgiFastStream())
    response = client.get("/")
    assert response.status_code == 404


def test_ws_not_found() -> None:
    client = TestClient(AsgiFastStream())
    with (
        pytest.raises(WebSocketDisconnect),
        client.websocket_connect("/ws"),
    ):  # raises error
        pass


def test_asyncapi_asgi() -> None:
    app = AsgiFastStream(specification=AsyncAPI(), asyncapi_path="/docs")

    client = TestClient(app)
    response = client.get("/docs")
    assert response.status_code == 200, response
    assert response.text


def test_asyncapi_json_default_path() -> None:
    app = AsgiFastStream(specification=AsyncAPI(), asyncapi_path="/docs")

    client = TestClient(app)
    response = client.get("/docs.json")
    assert response.status_code == 200, response
    assert response.headers["content-type"] == "application/json"
    assert response.json() == app.schema.to_specification().to_jsonable()


def test_asyncapi_json_path_strips_trailing_slash() -> None:
    app = AsgiFastStream(specification=AsyncAPI(), asyncapi_path="/docs/")

    client = TestClient(app)
    assert client.get("/docs.json").status_code == 200


def test_asyncapi_json_custom_path() -> None:
    app = AsgiFastStream(
        specification=AsyncAPI(),
        asyncapi_path=AsyncAPIRoute("/docs", asyncapi_json_path="/openapi.json"),
    )

    client = TestClient(app)
    response = client.get("/openapi.json")
    assert response.status_code == 200, response
    assert response.json() == app.schema.to_specification().to_jsonable()

    # the default derived path is not registered when overridden
    assert client.get("/docs.json").status_code == 404


def test_asyncapi_json_disabled() -> None:
    app = AsgiFastStream(
        specification=AsyncAPI(),
        asyncapi_path=AsyncAPIRoute("/docs", asyncapi_json_path=None),
    )

    client = TestClient(app)
    # docs HTML is still served
    assert client.get("/docs").status_code == 200
    # but the JSON endpoint is not registered
    assert client.get("/docs.json").status_code == 404


@pytest.mark.parametrize(
    ("decorator", "client_method"),
    (
        pytest.param(get, "get", id="get"),
        pytest.param(post, "post", id="post"),
    ),
)
def test_decorators(decorator: Callable[..., ASGIApp], client_method: str) -> None:
    @decorator
    async def some_handler(scope: Scope) -> AsgiResponse:
        return AsgiResponse(body=b"test", status_code=200)

    app = AsgiFastStream(asgi_routes=[("/test", some_handler)])

    client = TestClient(app)
    response = getattr(client, client_method)("/test")
    assert response.status_code == 200
    assert response.text == "test"


@pytest.mark.parametrize(
    ("decorator", "client_method"),
    (
        pytest.param(get, "get", id="get"),
        pytest.param(post, "post", id="post"),
    ),
)
def test_context_injected(decorator: Callable[..., ASGIApp], client_method: str) -> None:
    @decorator
    async def some_handler(
        request: Request, logger: Logger, app: FastStream
    ) -> AsgiResponse:
        return AsgiResponse(
            body=f"{request.__class__.__name__} {logger.__class__.__name__} {app.__class__.__name__}".encode(),
            status_code=200,
        )

    app = AsgiFastStream(asgi_routes=[("/test", some_handler)])

    client = TestClient(app)
    response = getattr(client, client_method)("/test")
    assert response.status_code == 200
    assert response.text == "AsgiRequest Logger AsgiFastStream"


@pytest.mark.parametrize(
    ("decorator", "client_method"),
    (
        pytest.param(get, "get", id="get"),
        pytest.param(post, "post", id="post"),
    ),
)
def test_fast_depends_injected(
    decorator: Callable[..., ASGIApp], client_method: str
) -> None:
    def get_string() -> str:
        return "test"

    @decorator
    async def some_handler(string=Depends(get_string)) -> AsgiResponse:  # noqa: B008
        return AsgiResponse(body=string.encode(), status_code=200)

    app = AsgiFastStream(asgi_routes=[("/test", some_handler)])

    client = TestClient(app)
    response = getattr(client, client_method)("/test")
    assert response.status_code == 200
    assert response.text == "test"


@pytest.mark.parametrize(
    "dependency",
    (
        pytest.param(Query(), id="query"),
        pytest.param(Header(), id="header"),
    ),
)
def test_validation_error_handled(dependency: Context) -> None:
    @get
    async def some_handler(dep=dependency) -> AsgiResponse:
        return AsgiResponse(status_code=200)

    app = AsgiFastStream(asgi_routes=[("/test", some_handler)])

    client = TestClient(app)
    response = client.get("/test")
    assert response.status_code == 422
    assert response.text == "Validation error"
