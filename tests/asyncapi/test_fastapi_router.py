from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from faststream.nats import TestNatsBroker
from faststream.nats.fastapi import NatsRouter


@pytest.mark.asyncio()
async def test_fastapi_asyncapi_not_fount() -> None:
    router = NatsRouter(include_in_schema=False)

    app = FastAPI()
    app.include_router(router)

    async with TestNatsBroker(router.broker):
        with TestClient(app) as client:
            response_json = client.get("/asyncapi.json")
            assert response_json.status_code == 404

            response_yaml = client.get("/asyncapi.yaml")
            assert response_yaml.status_code == 404

            response_html = client.get("/asyncapi")
            assert response_html.status_code == 404


@pytest.mark.asyncio()
async def test_fastapi_asyncapi_not_fount_by_url() -> None:
    router = NatsRouter(schema_url=None)

    app = FastAPI()
    app.include_router(router)

    async with TestNatsBroker(router.broker):
        with TestClient(app) as client:
            response_json = client.get("/asyncapi.json")
            assert response_json.status_code == 404

            response_yaml = client.get("/asyncapi.yaml")
            assert response_yaml.status_code == 404

            response_html = client.get("/asyncapi")
            assert response_html.status_code == 404


@pytest.mark.asyncio()
async def test_fastapi_asyncapi_routes() -> None:
    router = NatsRouter(schema_url="/asyncapi_schema")

    @router.subscriber("test")
    async def handler() -> None: ...

    app = FastAPI()
    app.include_router(router)

    async with TestNatsBroker(router.broker):
        with TestClient(app) as client:
            schema = router.schema.to_specification()

            response_json = client.get("/asyncapi_schema.json")
            assert response_json.json() == schema.to_jsonable()

            response_yaml = client.get("/asyncapi_schema.yaml")
            assert response_yaml.text == schema.to_yaml()

            response_html = client.get("/asyncapi_schema")
            assert response_html.status_code == 200


@pytest.mark.asyncio()
async def test_fastapi_asyncapi_try_it_out_delivers_message() -> None:
    """The try-it-out POST route must deliver a message to the broker (#2869)."""
    router = NatsRouter()
    mock = MagicMock()

    @router.subscriber("test-try")
    async def handler(msg: str) -> None:
        mock(msg)

    app = FastAPI()
    app.include_router(router)

    async with TestNatsBroker(router.broker):
        with TestClient(app) as client:
            response = client.post(
                "/asyncapi/try",
                json={
                    "channelName": "test-try",
                    "message": {
                        "operation_id": "op",
                        "operation_type": "subscribe",
                        "message": "hello",
                    },
                    "options": {"sendToRealBroker": False},
                },
            )
            assert response.status_code == 200, response.text

    mock.assert_called_once_with("hello")


@pytest.mark.asyncio()
async def test_fastapi_asyncapi_try_it_out_follows_schema_url() -> None:
    """The try-it-out POST route must track a custom ``schema_url``."""
    router = NatsRouter(schema_url="/asyncapi_schema")

    @router.subscriber("test-try")
    async def handler() -> None: ...

    app = FastAPI()
    app.include_router(router)

    async with TestNatsBroker(router.broker):
        with TestClient(app) as client:
            response = client.post(
                "/asyncapi_schema/try",
                json={
                    "channelName": "test-try",
                    "message": {
                        "operation_id": "op",
                        "operation_type": "subscribe",
                        "message": {},
                    },
                    "options": {"sendToRealBroker": False},
                },
            )
            assert response.status_code == 200, response.text

            # default schema_url should not expose the try route
            assert client.post("/asyncapi/try", json={}).status_code == 404
