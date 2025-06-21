from typing import Any
from unittest.mock import MagicMock

import msgspec
import pytest
from fast_depends.msgspec import MsgSpecSerializer

from faststream.rabbit import RabbitBroker, TestRabbitBroker
from tests.brokers.base.publish import parametrized


class SimpleModel(msgspec.Struct):
    r: str


@pytest.mark.rabbit()
@pytest.mark.asyncio()
@pytest.mark.parametrize(
    ("message", "message_type", "expected_message"),
    (
        *parametrized,
        pytest.param(
            msgspec.json.encode(SimpleModel(r="hello!")),
            SimpleModel,
            SimpleModel(r="hello!"),
            id="bytes->model",
        ),
        pytest.param(
            SimpleModel(r="hello!"),
            SimpleModel,
            SimpleModel(r="hello!"),
            id="model->model",
        ),
        pytest.param(
            SimpleModel(r="hello!"),
            dict,
            {"r": "hello!"},
            id="model->dict",
        ),
        pytest.param(
            {"r": "hello!"},
            SimpleModel,
            SimpleModel(r="hello!"),
            id="dict->model",
        ),
    ),
)
async def test_msgspec_serialize(
    message: Any,
    message_type: Any,
    expected_message: Any,
    mock: MagicMock,
) -> None:
    pub_broker = RabbitBroker(serializer=MsgSpecSerializer)

    @pub_broker.subscriber("test")
    async def handler(m: message_type) -> None:
        mock(m)

    async with TestRabbitBroker(pub_broker) as br:
        await br.publish(message, "test")

    mock.assert_called_with(expected_message)
