from collections.abc import Callable
from typing import Any

import pytest
from dirty_equals import IsStr
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from faststream._internal.broker import BrokerUsecase
from faststream._internal.fastapi.router import StreamRouter
from faststream._internal.types import MsgType
from faststream.specification import AsyncAPI


class FastAPITestCase:
    is_fastapi = True
    dependency_builder = staticmethod(Depends)

    router_class: type[StreamRouter[MsgType]]
    broker_wrapper: Callable[[BrokerUsecase[MsgType, Any]], BrokerUsecase[MsgType, Any]]

    @pytest.mark.asyncio()
    async def test_fastapi_full_information(self) -> None:
        router = self.router_class(
            protocol="custom",
            protocol_version="1.1.1",
            description="Test broker description",
            schema_url="/asyncapi_schema",
            specification_tags=[{"name": "test"}],
            specification=AsyncAPI(schema_version="2.6.0"),
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
                response_json = client.get("/asyncapi_schema.json").json()

                assert response_json == {
                    "asyncapi": "2.6.0",
                    "channels": {},
                    "components": {"messages": {}, "schemas": {}},
                    "defaultContentType": "application/json",
                    "info": {
                        "contact": {
                            "name": "support",
                            "url": IsStr(regex=r"https\:\/\/support\.com\/?"),
                        },
                        "description": "Test description",
                        "license": {
                            "name": "some",
                            "url": IsStr(regex=r"https\:\/\/some\.com\/?"),
                        },
                        "title": "CustomApp",
                        "version": "1.1.1",
                    },
                    "servers": {
                        "development": {
                            "description": "Test broker description",
                            "protocol": "custom",
                            "protocolVersion": "1.1.1",
                            "tags": [{"name": "test"}],
                            "url": IsStr(),
                        },
                    },
                }
