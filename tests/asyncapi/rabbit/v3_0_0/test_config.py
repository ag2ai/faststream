from typing import Any

import pytest

from faststream import Config
from faststream.rabbit import RabbitBroker, RabbitQueue
from tests.asyncapi.base.v3_0_0.config import ConfigTestcase


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
            config_values={"IN": RabbitQueue("q", routing_key="logs.{level}")},
        )
        configured.subscriber(Config("IN"))(handle)

        assert (
            self.get_spec(configured).to_jsonable()
            == self.get_spec(literal).to_jsonable()
        )

    def test_publisher_channel_names_the_resolved_routing_key(self) -> None:
        broker = self.get_broker(config_values={"OUT": "resolved-routing-key"})

        broker.publisher(routing_key=Config("OUT"))

        schema = self.get_spec(broker).to_jsonable()

        assert list(schema["channels"].keys()) == ["resolved-routing-key:_:Publisher"], (
            schema["channels"]
        )

    def test_publisher_schema_names_the_resolved_reply_to(self) -> None:
        broker = self.get_broker(config_values={"REPLY": "resolved-reply-address"})

        broker.publisher("q", reply_to=Config("REPLY"))

        schema = self.get_spec(broker).to_jsonable()

        assert (
            schema["operations"]["q:_:Publisher"]["bindings"]["amqp"]["replyTo"]
            == "resolved-reply-address"
        ), schema["operations"]
