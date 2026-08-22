from typing import Any

import pytest

from faststream import Config
from faststream.nats import NatsBroker
from tests.asyncapi.base.v2_6_0.config import ConfigTestcase


@pytest.mark.nats()
class TestConfigValues(ConfigTestcase):
    broker_class = NatsBroker

    def test_a_template_from_a_config_value_renders_as_a_literal_one(self) -> None:
        """This work does not change how an Address template is rendered (#2357)."""
        configured = self.get_broker(config={"IN": "logs.{level}"})

        @configured.subscriber(Config("IN"))
        async def from_config() -> None: ...

        literal = self.get_broker()

        @literal.subscriber("logs.{level}")
        async def from_literal() -> None: ...

        assert self._subjects(configured) == self._subjects(literal) == ["logs.{level}"]

    def _subjects(self, broker: Any) -> list[str]:
        schema = self.get_spec(broker).to_jsonable()
        return [
            channel["bindings"]["nats"]["subject"]
            for channel in schema["channels"].values()
        ]
