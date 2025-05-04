import pytest

from faststream import TestApp
from faststream.rabbit import TestRabbitBroker
from tests.marks import pydantic_v2
from tests.mocks import mock_pydantic_settings_env


@pydantic_v2
@pytest.mark.asyncio()
async def test() -> None:
    with mock_pydantic_settings_env(
        {"host": "amqp://guest:guest@localhost:5673/"},  # pragma: allowlist secret
    ):
        from docs.docs_src.getting_started.cli.rabbit_context import app, broker

        async with TestRabbitBroker(broker), TestApp(app, {"env": ".env"}):
            assert (
                app.context.get("settings").host
                == "amqp://guest:guest@localhost:5673/"  # pragma: allowlist secret
            )
