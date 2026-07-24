import pytest

from faststream.kafka.response import KafkaPublishCommand, KafkaPublishMessage
from faststream.response.publish_type import PublishType


def delivered(cmd):
    return [(body, cmd.key_for(i)) for i, body in enumerate(cmd.batch_bodies)]


@pytest.mark.kafka()
def test_keys_order() -> None:
    cmd = KafkaPublishCommand(
        KafkaPublishMessage("body-A", key=b"key-A"),
        KafkaPublishMessage("body-B", key=b"key-B"),
        topic="topic",
        _publish_type=PublishType.PUBLISH,
    )
    cmd.batch_bodies = tuple(reversed(cmd.batch_bodies))
    delivered_cmd = delivered(cmd)
    assert delivered_cmd == [("body-B", b"key-B"), ("body-A", b"key-A")]
