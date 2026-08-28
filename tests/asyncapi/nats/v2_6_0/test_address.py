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

from faststream.nats import NatsBroker
from tests.asyncapi.base.v2_6_0.basic import get_2_6_0_schema


@pytest.mark.nats()
def test_every_address_is_named_as_declared() -> None:
    broker = NatsBroker()

    @broker.subscriber("logs.{level}")
    async def handle_logs(body: str) -> None: ...

    broker.publisher("cache{{shard}}")

    schema = get_2_6_0_schema(broker)

    assert schema["channels"] == {
        "logs.{level}:HandleLogs": {
            "servers": ["development"],
            "bindings": {"nats": {"subject": "logs.{level}", "bindingVersion": "custom"}},
            "publish": {
                "message": {
                    "$ref": "#/components/messages/logs.%7Blevel%7D:HandleLogs:Message"
                }
            },
        },
        "cache{shard}:Publisher": {
            "servers": ["development"],
            "bindings": {"nats": {"subject": "cache{shard}", "bindingVersion": "custom"}},
            "subscribe": {
                "message": {
                    "$ref": "#/components/messages/cache%7Bshard%7D:Publisher:Message"
                }
            },
        },
    }
