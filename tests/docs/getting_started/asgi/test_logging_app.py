import pytest
from starlette.testclient import TestClient

from faststream.nats import TestNatsBroker


@pytest.mark.nats()
@pytest.mark.asyncio()
async def test_logging_app() -> None:
    from docs.docs_src.getting_started.asgi.logging_app import app, broker

    async with TestNatsBroker(broker):
        with TestClient(app) as client:
            response = client.post("/log-request-payload", json={"a": "b"})
            assert response.status_code == 200
