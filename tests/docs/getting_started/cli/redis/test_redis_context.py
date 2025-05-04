import pytest

from faststream import TestApp
from faststream.redis import TestRedisBroker
from tests.marks import pydantic_v2
from tests.mocks import mock_pydantic_settings_env


@pydantic_v2
@pytest.mark.asyncio()
async def test() -> None:
    with mock_pydantic_settings_env({"host": "redis://localhost:6380"}):
        from docs.docs_src.getting_started.cli.redis_context import app, broker

        async with TestRedisBroker(broker), TestApp(app, {"env": ".env"}):
            assert app.context.get("settings").host == "redis://localhost:6380"
