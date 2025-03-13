from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from tests.brokers.base.fastapi import FastAPITestcase as DefaultFastAPITestcase


class Data(BaseModel): ...


class FastapiTestCase(DefaultFastAPITestcase):
    async def test_forward_ref(self, queue: str):
        # see https://github.com/airtai/faststream/issues/2077 for context

        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def h(data: Data) -> None: ...

        app = FastAPI()
        app.include_router(router)

        with TestClient(app) as client:
            response = client.get("/asyncapi.json")
            assert response.status_code == 200
            assert "Data" in response.json()["components"]["schemas"]
