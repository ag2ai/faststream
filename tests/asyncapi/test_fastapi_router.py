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
