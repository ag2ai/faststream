from typing import Any

import pytest

from faststream import Config
from faststream.redis import RedisBroker
from tests.asyncapi.base.v2_6_0.config import ConfigTestcase


@pytest.mark.redis()
class TestConfigValues(ConfigTestcase):
    broker_class = RedisBroker

    def test_a_template_from_a_config_value_renders_as_a_literal_one(self) -> None:
        """An Address template documents the same contract wherever it came from.

        How a template renders is ag2ai/faststream#2357's question, not this
        work's: a Config value must not answer it differently.
        """

        def declare(broker: Any, address: Any) -> None:
            @broker.subscriber(address)
            async def handle() -> None: ...

        from_config = self.get_broker(config={"IN": "logs.{level}"})
        declare(from_config, Config("IN"))

        literal = self.get_broker()
        declare(literal, "logs.{level}")

        assert (
            self.get_spec(from_config).to_jsonable()["channels"]
            == self.get_spec(literal).to_jsonable()["channels"]
        )
