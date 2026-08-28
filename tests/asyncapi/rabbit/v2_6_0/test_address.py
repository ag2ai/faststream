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

from faststream.rabbit import (
    ExchangeType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)
from tests.asyncapi.base.v2_6_0.basic import get_2_6_0_schema

EXCHANGE = RabbitExchange("logs-ex", type=ExchangeType.TOPIC)


@pytest.mark.rabbit()
def test_every_address_is_named_as_declared() -> None:
    broker = RabbitBroker()

    @broker.subscriber(RabbitQueue("logs-q", routing_key="logs.{level}"), EXCHANGE)
    async def handle_logs(body: str) -> None: ...

    broker.publisher(routing_key="cache{{shard}}", exchange=EXCHANGE)

    schema = get_2_6_0_schema(broker)

    assert schema["channels"] == {
        "logs-q:logs-ex:HandleLogs": {
            "servers": ["development"],
            "bindings": {
                "amqp": {
                    "is": "routingKey",
                    "bindingVersion": "0.2.0",
                    "queue": {
                        "name": "logs-q",
                        "durable": True,
                        "exclusive": False,
                        "autoDelete": False,
                        "vhost": "/",
                    },
                    "exchange": {
                        "name": "logs-ex",
                        "type": "topic",
                        "durable": True,
                        "autoDelete": False,
                        "vhost": "/",
                    },
                }
            },
            "publish": {
                "bindings": {
                    "amqp": {"cc": "logs.{level}", "ack": True, "bindingVersion": "0.2.0"}
                },
                "message": {
                    "$ref": "#/components/messages/logs-q:logs-ex:HandleLogs:Message"
                },
            },
        },
        "cache{shard}:logs-ex:Publisher": {
            "servers": ["development"],
            "bindings": {
                "amqp": {
                    "is": "routingKey",
                    "bindingVersion": "0.2.0",
                    "exchange": {
                        "name": "logs-ex",
                        "type": "topic",
                        "durable": True,
                        "autoDelete": False,
                        "vhost": "/",
                    },
                }
            },
            "subscribe": {
                "bindings": {
                    "amqp": {
                        "cc": "cache{shard}",
                        "ack": True,
                        "deliveryMode": 1,
                        "mandatory": True,
                        "bindingVersion": "0.2.0",
                    }
                },
                "message": {
                    "$ref": "#/components/messages/cache%7Bshard%7D:logs-ex:Publisher:Message"
                },
            },
        },
    }
