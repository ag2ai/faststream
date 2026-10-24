from typing import Any

import pytest

from faststream.mqtt import MQTTBroker
from tests.asyncapi.base.v2_6_0.arguments import ArgumentsTestcase


@pytest.mark.mqtt()
class TestArguments(ArgumentsTestcase):
    broker_class = MQTTBroker

    def test_subscriber_bindings(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg: Any) -> None: ...

        schema = self.get_spec(broker).to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # ruff: ignore[unnecessary-iterable-allocation-for-first-element]

        assert schema["channels"][key]["bindings"] == {
            "mqtt": {
                "bindingVersion": "0.2.0",
                "qos": 0,
                "retain": False,
                "topic": "test",
            },
        }, schema["channels"][key]["bindings"]
