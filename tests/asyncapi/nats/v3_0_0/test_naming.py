import pytest
from nats.js.api import ConsumerConfig

from faststream.nats import JStream, NatsBroker, PullSub
from tests.asyncapi.base.v3_0_0.naming import NamingTestCase


@pytest.mark.nats()
class TestNaming(NamingTestCase):
    broker_class = NatsBroker

    def test_filter_subjects_without_subject(self) -> None:
        """A JetStream consumer may address a stream through `filter_subjects` and no `subject`."""
        broker = self.broker_class()

        @broker.subscriber(
            stream=JStream("stream"),
            pull_sub=PullSub(),
            durable="durable",
            config=ConsumerConfig(filter_subjects=["logs.{level}"]),
        )
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        (channel_name,) = schema["channels"]
        assert channel_name == "logs.{level}:Handle"
        # one filtered subject, so that subject is the address
        assert schema["channels"][channel_name]["address"] == "logs.{level}"
        assert (
            schema["channels"][channel_name]["bindings"]["nats"]["subject"]
            == "logs.{level}"
        )

    def test_multiple_filter_subjects_without_subject(self) -> None:
        """Several filtered subjects are rendered the same way the runtime reports them."""
        broker = self.broker_class()

        @broker.subscriber(
            stream=JStream("stream"),
            pull_sub=PullSub(),
            durable="durable",
            config=ConsumerConfig(filter_subjects=["logs.info", "logs.error"]),
        )
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        (channel_name,) = schema["channels"]
        assert (
            schema["channels"][channel_name]["bindings"]["nats"]["subject"]
            == "logs.info, logs.error"
        )
        # Two filtered subjects, so neither one of them is *the* address, and an
        # absent address is the only way to say that.
        assert "address" not in schema["channels"][channel_name]

    def test_base(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == {
            "asyncapi": "3.0.0",
            "defaultContentType": "application/json",
            "info": {"title": "FastStream", "version": "0.1.0"},
            "servers": {
                "development": {
                    "host": "localhost:4222",
                    "pathname": "",
                    "protocol": "nats",
                    "protocolVersion": "custom",
                },
            },
            "channels": {
                "test:Handle": {
                    "address": "test",
                    "servers": [
                        {
                            "$ref": "#/servers/development",
                        },
                    ],
                    "bindings": {
                        "nats": {"subject": "test", "bindingVersion": "custom"},
                    },
                    "messages": {
                        "SubscribeMessage": {
                            "$ref": "#/components/messages/test:Handle:SubscribeMessage",
                        },
                    },
                },
            },
            "operations": {
                "test:HandleSubscribe": {
                    "action": "receive",
                    "channel": {
                        "$ref": "#/channels/test:Handle",
                    },
                    "messages": [
                        {
                            "$ref": "#/channels/test:Handle/messages/SubscribeMessage",
                        },
                    ],
                },
            },
            "components": {
                "messages": {
                    "test:Handle:SubscribeMessage": {
                        "title": "test:Handle:SubscribeMessage",
                        "correlationId": {
                            "location": "$message.header#/correlation_id",
                        },
                        "payload": {"$ref": "#/components/schemas/EmptyPayload"},
                    },
                },
                "schemas": {"EmptyPayload": {"title": "EmptyPayload", "type": "null"}},
            },
        }
