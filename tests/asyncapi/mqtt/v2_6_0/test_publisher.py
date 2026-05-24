import pytest

from faststream.mqtt import MQTTBroker
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase


@pytest.mark.mqtt()
class TestArguments(PublisherTestcase):
    broker_class = MQTTBroker

    def test_publisher_bindings(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test")
        async def handle(msg) -> None: ...

        schema = self.get_spec(broker).to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert schema["channels"][key]["bindings"] == {
            "mqtt": {
                "bindingVersion": "0.2.0",
                "qos": 0,
                "retain": False,
                "topic": "test",
            },
        }, schema["channels"][key]["bindings"]
