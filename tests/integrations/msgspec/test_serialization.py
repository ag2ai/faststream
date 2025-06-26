from typing import Any
from unittest.mock import MagicMock

import msgspec
import pytest
from fast_depends.msgspec import MsgSpecSerializer

from faststream._internal.broker.broker import BrokerUsecase
from faststream._internal.testing.broker import TestBroker
from faststream.confluent import (
    KafkaBroker as ConfluentBroker,
    TestKafkaBroker as TestConfluentBroker,
)
from faststream.kafka import KafkaBroker, TestKafkaBroker
from faststream.nats import NatsBroker, TestNatsBroker
from faststream.rabbit import RabbitBroker, TestRabbitBroker
from faststream.redis import RedisBroker, TestRedisBroker
from tests.brokers.base.publish import parametrized


class SimpleModel(msgspec.Struct):
    r: str


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
@pytest.mark.parametrize(
    ("broker_cls", "test_cls"),
    (
        pytest.param(RabbitBroker, TestRabbitBroker, id="rabbit"),
        pytest.param(RedisBroker, TestRedisBroker, id="redis"),
        pytest.param(KafkaBroker, TestKafkaBroker, id="kafka"),
        pytest.param(NatsBroker, TestNatsBroker, id="nats"),
        pytest.param(ConfluentBroker, TestConfluentBroker, id="confluent"),
    ),
)
async def test_msgspec_serialize(
    message: Any,
    message_type: Any,
    expected_message: Any,
    mock: MagicMock,
    broker_cls: type[BrokerUsecase[Any, Any]],
    test_cls: type[TestBroker[Any]],
) -> None:
    broker = broker_cls(serializer=MsgSpecSerializer)

    @broker.subscriber("test")
    async def handler(m: message_type) -> None:
        mock(m)

    async with test_cls(broker) as br:
        await br.publish(message, "test")

    mock.assert_called_with(expected_message)
