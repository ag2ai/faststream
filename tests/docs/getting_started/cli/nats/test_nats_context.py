import pytest

from faststream import TestApp
from faststream.nats import TestNatsBroker
from tests.marks import pydantic_v2
from tests.mocks import mock_pydantic_settings_env


@pydantic_v2
@pytest.mark.asyncio()
async def test() -> None:
    with mock_pydantic_settings_env({"host": "localhost"}):
        from docs.docs_src.getting_started.cli.nats_context import app, broker

        async with TestNatsBroker(broker), TestApp(app, {"env": ""}):
            assert app.context.get("settings").host == "localhost"
