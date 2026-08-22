import pytest

from faststream import Config
from faststream.mqtt import MQTTBroker
from tests.asyncapi.base.v3_0_0.config import ConfigTestcase


@pytest.mark.mqtt()
class TestConfigValues(ConfigTestcase):
    broker_class = MQTTBroker

    def test_a_template_renders_the_same_whichever_way_it_arrived(self) -> None:
        """An Address template's rendering is #2357's business, not this work's."""

        async def handle() -> None: ...

        configured = self.get_broker(config={"IN": "logs/{level}"})
        configured.subscriber(Config("IN"))(handle)

        literal = self.get_broker()
        literal.subscriber("logs/{level}")(handle)

        schema = self.get_spec(configured).to_jsonable()

        assert schema == self.get_spec(literal).to_jsonable()
        assert any("{level}" in key for key in schema["channels"]), schema["channels"]
