import pytest

from faststream import Config
from faststream.confluent import KafkaBroker
from tests.asyncapi.base.v2_6_0.config import ConfigTestcase


@pytest.mark.confluent()
class TestConfigValues(ConfigTestcase):
    broker_class = KafkaBroker

    def test_a_template_renders_the_same_from_a_config_value(self) -> None:
        """A Config value changes where the address comes from, not how it renders.

        ag2ai/faststream#2357 is about what a channel holding an Address template
        should look like; whatever it settles on has to be one answer, not one per
        source.
        """

        async def handle() -> None: ...

        literal = self.get_broker()
        literal.subscriber("logs.{level}")(handle)

        resolved = self.get_broker(config={"IN": "logs.{level}"})
        resolved.subscriber(Config("IN"))(handle)

        assert (
            self.get_spec(resolved).to_jsonable()["channels"]
            == self.get_spec(literal).to_jsonable()["channels"]
        )
