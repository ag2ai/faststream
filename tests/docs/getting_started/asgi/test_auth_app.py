import pytest
from starlette.testclient import TestClient

from faststream.nats import TestNatsBroker


@pytest.mark.nats()
@pytest.mark.asyncio()
async def test_auth_app() -> None:
    from docs.docs_src.getting_started.asgi.auth_app import app, broker

    async with TestNatsBroker(broker):
        with TestClient(app) as client:
            response = client.get(
                "/protected-method?foo=1", headers={"X-auth-token": "wrong-token"}
            )
            assert response.status_code == 401

            response = client.get(
                "/protected-method?foo=1", headers={"X-auth-token": "secret-token"}
            )
            assert response.status_code == 200
