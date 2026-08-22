from typing import Any

import pytest

from faststream import Config
from faststream.rabbit import RabbitBroker, RabbitQueue
from tests.asyncapi.base.v2_6_0.config import ConfigTestcase


@pytest.mark.rabbit()
class TestConfigValues(ConfigTestcase):
    broker_class = RabbitBroker

    def test_a_template_renders_as_the_literal_spelling_would(self) -> None:
        """A template out of a Config value documents what a literal one does.

        Config values do not change how an Address template is rendered; whatever
        ag2ai/faststream#2357 decides that rendering should be, both spellings
        get the same one.
        """

        async def handle(msg: Any) -> None: ...

        literal = RabbitBroker()
        literal.subscriber(RabbitQueue("q", routing_key="logs.{level}"))(handle)

        configured = RabbitBroker(
            config={"IN": RabbitQueue("q", routing_key="logs.{level}")},
        )
        configured.subscriber(Config("IN"))(handle)

        assert (
            self.get_spec(configured).to_jsonable()
            == self.get_spec(literal).to_jsonable()
        )
