"""The document names every address exactly as it was declared.

A template documents a contract — `logs.{level}` is what a reader of the
document has to write to reach this endpoint — while the Broker address it
compiles to is one broker's way of asking for that family and means nothing to
anybody else. `{{shard}}` is FastStream's own escape for a literal brace, and no
reader of the document knows about it either.

The whole `channels` and `operations` documents are pinned rather than probed:
an address that is right in a binding and wrong in a channel key is still a
wrong document, and only comparing the lot catches it.

Kafka is the exception on the escape: a topic is never compiled, so `{{shard}}`
is not syntax there and reaches the document — and the wire — as it was typed.
Only `pattern=` goes through the parameter parser, and only it has an escape to
undo. Pinned here so the difference is a decision rather than a surprise.
"""

import pytest

from faststream.kafka import KafkaBroker
from tests.asyncapi.base.v3_0_0.basic import get_3_0_0_schema


@pytest.mark.kafka()
def test_every_address_is_named_as_declared() -> None:
    broker = KafkaBroker()

    @broker.subscriber(pattern="logs.{level}")
    async def handle_logs(body: str) -> None: ...

    broker.publisher("cache{{shard}}")

    schema = get_3_0_0_schema(broker)

    assert schema["channels"] == {
        "logs.{level}:HandleLogs": {
            "address": "logs.{level}:HandleLogs",
            "servers": [{"$ref": "#/servers/development"}],
            "messages": {
                "SubscribeMessage": {
                    "$ref": "#/components/messages/logs.%7Blevel%7D:HandleLogs:SubscribeMessage"
                }
            },
            "bindings": {"kafka": {"topic": "logs.{level}", "bindingVersion": "0.4.0"}},
        },
        "cache{{shard}}:Publisher": {
            "address": "cache{{shard}}:Publisher",
            "servers": [{"$ref": "#/servers/development"}],
            "messages": {
                "Message": {
                    "$ref": "#/components/messages/cache%7B%7Bshard%7D%7D:Publisher:Message"
                }
            },
            "bindings": {"kafka": {"topic": "cache{{shard}}", "bindingVersion": "0.4.0"}},
        },
    }

    assert schema["operations"] == {
        "logs.{level}:HandleLogsSubscribe": {
            "action": "receive",
            "channel": {"$ref": "#/channels/logs.%7Blevel%7D:HandleLogs"},
            "messages": [
                {
                    "$ref": "#/channels/logs.%7Blevel%7D:HandleLogs/messages/SubscribeMessage"
                }
            ],
        },
        "cache{{shard}}:Publisher": {
            "action": "send",
            "channel": {"$ref": "#/channels/cache%7B%7Bshard%7D%7D:Publisher"},
            "messages": [
                {"$ref": "#/channels/cache%7B%7Bshard%7D%7D:Publisher/messages/Message"}
            ],
        },
    }
