import pytest

from faststream import TestApp
from faststream.confluent import TestKafkaBroker
from tests.marks import pydantic_v2
from tests.mocks import mock_pydantic_settings_env


@pydantic_v2
@pytest.mark.asyncio()
async def test() -> None:
    with mock_pydantic_settings_env({"host": "localhost"}):
        from docs.docs_src.getting_started.cli.confluent_context import app, broker

        async with TestKafkaBroker(broker), TestApp(app, {"env": ""}):
            assert app.context.get("settings").host == "localhost"
