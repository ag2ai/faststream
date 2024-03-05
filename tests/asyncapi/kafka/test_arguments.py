from faststream.asyncapi.generate import get_app_schema
from faststream.kafka import KafkaBroker
from tests.asyncapi.base.arguments import ArgumentsTestcase


class TestArguments(ArgumentsTestcase):  # noqa: D101
    broker_class = KafkaBroker

    def test_subscriber_bindings(self):
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert schema["channels"][key]["bindings"] == {
            "kafka": {"bindingVersion": "0.4.0", "topic": "test"}
        }
