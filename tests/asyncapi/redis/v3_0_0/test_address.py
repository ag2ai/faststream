"""The document names every address exactly as it was declared.

A template documents a contract — `logs.{level}` is what a reader of the
document has to write to reach this endpoint — while the Broker address it
compiles to is one broker's way of asking for that family and means nothing to
anybody else. `{{shard}}` is FastStream's own escape for a literal brace, and no
reader of the document knows about it either.

The whole `channels` and `operations` documents are pinned rather than probed:
an address that is right in a binding and wrong in a channel key is still a
wrong document, and only comparing the lot catches it.
"""

import pytest

from faststream.redis import RedisBroker
from tests.asyncapi.base.v3_0_0.basic import get_3_0_0_schema


@pytest.mark.redis()
def test_every_address_is_named_as_declared() -> None:
    broker = RedisBroker()

    @broker.subscriber("logs.{level}")
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
            "bindings": {
                "redis": {
                    "channel": "logs.{level}",
                    "method": "psubscribe",
                    "bindingVersion": "custom",
                }
            },
        },
        "cache{shard}:Publisher": {
            "address": "cache{shard}:Publisher",
            "servers": [{"$ref": "#/servers/development"}],
            "messages": {
                "Message": {
                    "$ref": "#/components/messages/cache%7Bshard%7D:Publisher:Message"
                }
            },
            "bindings": {
                "redis": {
                    "channel": "cache{shard}",
                    "method": "publish",
                    "bindingVersion": "custom",
                }
            },
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
        "cache{shard}:Publisher": {
            "action": "send",
            "channel": {"$ref": "#/channels/cache%7Bshard%7D:Publisher"},
            "messages": [
                {"$ref": "#/channels/cache%7Bshard%7D:Publisher/messages/Message"}
            ],
        },
    }
