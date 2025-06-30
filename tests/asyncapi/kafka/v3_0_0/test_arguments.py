from faststream.kafka import KafkaBroker
from faststream.specification.asyncapi import AsyncAPI
from tests.asyncapi.base.v3_0_0.arguments import ArgumentsTestcase


class TestArguments(ArgumentsTestcase):
    broker_factory = KafkaBroker

    def test_subscriber_bindings(self) -> None:
        broker = self.broker_factory()

        @broker.subscriber("test")
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="3.0.0").to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert schema["channels"][key]["bindings"] == {
            "kafka": {"bindingVersion": "0.4.0", "topic": "test"},
        }
