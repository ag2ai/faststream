from typing import Any, Callable

import pytest
from dirty_equals import IsStr
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from faststream._internal.broker.broker import BrokerUsecase
from faststream._internal.fastapi.router import StreamRouter
from faststream._internal.types import MsgType
from faststream.specification.asyncapi import AsyncAPI


class FastAPITestCase:
    is_fastapi = True

    router_class: type[StreamRouter[MsgType]]
    broker_wrapper: Callable[[BrokerUsecase[MsgType, Any]], BrokerUsecase[MsgType, Any]]

    dependency_builder = staticmethod(Depends)

    @pytest.mark.skip()
    @pytest.mark.asyncio()
    async def test_fastapi_full_information(self) -> None:
        router = self.router_class(
            protocol="custom",
            protocol_version="1.1.1",
            description="Test broker description",
            schema_url="/asyncapi_schema",
            specification_tags=[{"name": "test"}],
        )

        app = FastAPI(
            title="CustomApp",
            version="1.1.1",
            description="Test description",
            contact={"name": "support", "url": "https://support.com"},
            license_info={"name": "some", "url": "https://some.com"},
        )
        app.include_router(router)

        async with self.broker_wrapper(router.broker):
            with TestClient(app) as client:
                response_json = client.get("/asyncapi_schema.json")

                assert response_json.json() == {
                    "asyncapi": "2.6.0",
                    "defaultContentType": "application/json",
                    "info": {
                        "title": "CustomApp",
                        "version": "1.1.1",
                        "description": "Test description",
                        "contact": {
                            "name": "support",
                            "url": IsStr(regex=r"https\:\/\/support\.com\/?"),
                        },
                        "license": {
                            "name": "some",
                            "url": IsStr(regex=r"https\:\/\/some\.com\/?"),
                        },
                    },
                    "servers": {
                        "development": {
                            "url": IsStr(),
                            "protocol": "custom",
                            "description": "Test broker description",
                            "protocolVersion": "1.1.1",
                            "tags": [{"name": "test"}],
                        },
                    },
                    "channels": {},
                    "components": {"messages": {}, "schemas": {}},
                }

    @pytest.mark.skip()
    @pytest.mark.asyncio()
    async def test_fastapi_asyncapi_routes(self) -> None:
        router = self.router_class(schema_url="/asyncapi_schema")

        @router.subscriber("test")
        async def handler() -> None: ...

        app = FastAPI()
        app.include_router(router)

        async with self.broker_wrapper(router.broker):
            with TestClient(app) as client:
                schema = AsyncAPI(router.broker, schema_version="2.6.0")

                response_json = client.get("/asyncapi_schema.json")
                assert response_json.json() == schema.to_jsonable()

                response_yaml = client.get("/asyncapi_schema.yaml")
                assert response_yaml.text == schema.to_yaml()

                response_html = client.get("/asyncapi_schema")
                assert response_html.status_code == 200

    @pytest.mark.asyncio()
    async def test_fastapi_asyncapi_not_fount(self) -> None:
        router = self.router_class(include_in_schema=False)

        app = FastAPI()
        app.include_router(router)

        async with self.broker_wrapper(router.broker):
            with TestClient(app) as client:
                response_json = client.get("/asyncapi.json")
                assert response_json.status_code == 404

                response_yaml = client.get("/asyncapi.yaml")
                assert response_yaml.status_code == 404

                response_html = client.get("/asyncapi")
                assert response_html.status_code == 404

    @pytest.mark.asyncio()
    async def test_fastapi_asyncapi_not_fount_by_url(self) -> None:
        router = self.router_class(schema_url=None)

        app = FastAPI()
        app.include_router(router)

        async with self.broker_wrapper(router.broker):
            with TestClient(app) as client:
                response_json = client.get("/asyncapi.json")
                assert response_json.status_code == 404

                response_yaml = client.get("/asyncapi.yaml")
                assert response_yaml.status_code == 404

                response_html = client.get("/asyncapi")
                assert response_html.status_code == 404
