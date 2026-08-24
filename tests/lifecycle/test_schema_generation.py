"""Schema generation drives Preparation before it renders.

Kafka is the reference Broker for these rules. The contract lives in the shared
internals and is identical for all six Brokers, so asserting it six times would be
duplication rather than coverage.

`faststream docs gen` never opens a connection, so the static checks a start-up runs
used to be missed entirely by CI. Rendering a schema is a read of resolved values, so
it is the moment a CLI run has instead of a start-up.

The endpoint a schema leaves out is prepared with the rest of its Broker; that case
lives in the shared Config testcase, where every Broker inherits it.
"""

from typing import Any
from unittest.mock import patch

import pytest
from starlette.applications import Starlette
from starlette.routing import Mount
from starlette.testclient import TestClient
from typer.testing import CliRunner

from faststream import Config, FastStream, Path
from faststream._internal.cli.main import cli as faststream_app
from faststream._internal.parser import DefaultCodec
from faststream.asgi import make_asyncapi_asgi
from faststream.exceptions import IncorrectState, SetupError
from faststream.kafka import KafkaBroker, TestKafkaBroker
from faststream.specification import AsyncAPI

pytestmark = pytest.mark.kafka()


def generate_docs(runner: CliRunner, app: FastStream) -> Any:
    """Run `faststream docs gen` over an already-built App."""
    with patch(
        "faststream._internal.cli.utils.imports._import_object_or_factory",
        return_value=(None, app),
    ):
        return runner.invoke(
            faststream_app,
            ["docs", "gen", "faststream:app", "--debug"],
        )


def test_generation_fails_on_a_missing_config_value(runner: CliRunner) -> None:
    broker = KafkaBroker()

    @broker.subscriber(Config("IN"))
    async def handler(msg: Any) -> None: ...

    result = generate_docs(runner, FastStream(broker))

    assert result.exit_code != 0
    assert isinstance(result.exception, SetupError)
    assert "IN" in str(result.exception)


def test_generation_fails_on_an_unsatisfiable_path_parameter(
    runner: CliRunner,
) -> None:
    broker = KafkaBroker()

    @broker.subscriber("logs")
    async def handler(msg: Any, level: str = Path()) -> None: ...

    result = generate_docs(runner, FastStream(broker))

    assert result.exit_code != 0
    assert isinstance(result.exception, SetupError)
    assert "level" in str(result.exception)


@pytest.mark.asyncio()
async def test_rendering_a_schema_twice_leaves_the_broker_working(queue: str) -> None:
    """A render is repeatable, and leaves the Broker able to consume.

    The `codec` declaration is the probe: composing it a second time is what a
    second Preparation would do, and that raises. Preparation's own idempotence
    is asserted in `test_preparation.py`.
    """
    broker = KafkaBroker()

    @broker.subscriber(queue, codec=DefaultCodec())
    async def handler(msg: Any) -> None: ...

    app = FastStream(broker, specification=AsyncAPI(schema_version="3.0.0"))

    assert app.schema.to_specification().to_jsonable() == (
        app.schema.to_specification().to_jsonable()
    )

    async with TestKafkaBroker(broker) as br:
        await br.publish("hello", queue)
        handler.mock.assert_called_once_with("hello")


def test_the_documented_asgi_mount_renders_on_first_request() -> None:
    """`AsyncAPI(broker)` mounted into a third-party ASGI application."""
    broker = KafkaBroker()

    @broker.subscriber("topic")
    async def handler(msg: str) -> None: ...

    app = Starlette(
        routes=[Mount("/docs/asyncapi", make_asyncapi_asgi(AsyncAPI(broker)))],
    )

    with TestClient(app) as client:
        response = client.get("/docs/asyncapi")

    assert response.status_code == 200, response
    assert "FastStream AsyncAPI" in response.text


@pytest.mark.asyncio()
async def test_a_render_leaves_no_preparation_behind(queue: str) -> None:
    """Preparation belongs to the connection it precedes, and a render opens none.

    Left behind, it would pin every address at the moment the schema was
    written: the `connect()` that followed would find the endpoints prepared,
    do nothing, and consume from whatever Config values were in scope back
    then rather than the ones the application was composed with.
    """
    broker = KafkaBroker(config_values={"IN": queue})

    @broker.subscriber(Config("IN"))
    async def handler(msg: Any) -> None: ...

    AsyncAPI(broker).to_specification()

    with pytest.raises(IncorrectState):
        _ = broker.subscribers[0].topics

    async with TestKafkaBroker(broker) as br:
        assert broker.subscribers[0].topics == [queue]

        await br.publish("hello", queue)
        handler.mock.assert_called_once_with("hello")


@pytest.mark.asyncio()
async def test_a_running_broker_keeps_its_preparation_across_a_render(
    queue: str,
) -> None:
    """The mounted-`AsyncAPI` case: a render must not disturb what is consuming.

    The counterpart of the case above -- undoing Preparation unconditionally
    would pull the addresses out from under Subscribers already running.
    """
    broker = KafkaBroker()

    @broker.subscriber(queue)
    async def handler(msg: Any) -> None: ...

    async with TestKafkaBroker(broker) as br:
        AsyncAPI(broker).to_specification()

        assert broker.subscribers[0].topics == [queue]

        await br.publish("hello", queue)
        handler.mock.assert_called_once_with("hello")
