import pytest

from faststream.mqtt import MQTTBroker
from tests.asyncapi.base.v3_0_0.basic import get_3_0_0_schema


@pytest.mark.mqtt()
def test_a_shared_subscription_is_not_part_of_the_address() -> None:
    """`$share/<group>/` asks for a shared subscription; it is not part of a topic.

    A message delivered here carries `test`, never `$share/group/test`, so that is
    what the address says. The group is not lost from the document: the channel
    name keeps it, and so does `bindings.mqtt.topic`.
    """
    broker = MQTTBroker()

    @broker.subscriber("test", shared="group")
    async def handle(body: str) -> None: ...

    ((name, channel),) = get_3_0_0_schema(broker)["channels"].items()

    assert channel["address"] == "test"

    # 3.0 channel keys are cleaned of the characters a `$ref` cannot hold, which is
    # why the name reads `$share.group.test` rather than the topic itself.
    assert name == "$share.group.test:Handle"
    assert channel["bindings"]["mqtt"]["topic"] == "$share/group/test"
