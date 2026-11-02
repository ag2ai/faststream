from typing import Any

import pytest

from faststream.confluent import KafkaBroker
from tests.asyncapi.base.v3_0_0.arguments import ArgumentsTestcase


@pytest.mark.confluent()
class TestArguments(ArgumentsTestcase):
    broker_class = KafkaBroker

    def test_subscriber_bindings(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg: Any) -> None: ...

        schema = self.get_spec(broker).to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # ruff: ignore[unnecessary-iterable-allocation-for-first-element]

        assert schema["channels"][key]["bindings"] == {
            "kafka": {"bindingVersion": "0.4.0", "topic": "test"},
        }
