import pytest

from faststream.sqs import SQSBroker
from tests.asyncapi.base.v2_6_0.arguments import ArgumentsTestcase


@pytest.mark.sqs()
class TestArguments(ArgumentsTestcase):
    broker_class = SQSBroker

    def test_subscriber_bindings(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg) -> None: ...

        schema = self.get_spec(broker).to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert schema["channels"][key]["bindings"] == {
            "sqs": {
                "queue": {"name": "test", "fifo": False},
                "bindingVersion": "custom",
            },
        }, schema["channels"][key]["bindings"]
