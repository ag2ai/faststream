import pytest

from faststream.kafka.response import KafkaPublishCommand, KafkaPublishMessage
from faststream.response.publish_type import PublishType


def delivered(cmd):
    return [(body, cmd.key_for(i)) for i, body in enumerate(cmd.batch_bodies)]


def reverse_bodies(batch_bodies):
    return tuple(reversed(batch_bodies))


def remove_body(batch_bodies):
    return tuple(batch_bodies[1:])


def do_nothing(batch_bodies):
    return batch_bodies


@pytest.mark.kafka()
@pytest.mark.parametrize(
    ("kafka_messages", "changing_pattern", "expected"),
    (
        (
            [
                KafkaPublishMessage("body-A", key=b"key-A"),
                KafkaPublishMessage("body-B", key=b"key-B"),
            ],
            reverse_bodies,
            [("body-B", b"key-B"), ("body-A", b"key-A")],
        ),
        (
            [
                KafkaPublishMessage("body-A", key=b"key-A"),
                KafkaPublishMessage("body-B", key=b"key-B"),
                KafkaPublishMessage("body-C", key=b"key-C"),
            ],
            remove_body,
            [("body-B", b"key-B"), ("body-C", b"key-C")],
        ),
        (
            [
                KafkaPublishMessage("body-A", key=b"key-A"),
                KafkaPublishMessage("body-B", key=b"key-B"),
                KafkaPublishMessage("body-B", key=b"key-B"),
            ],
            reverse_bodies,
            [("body-B", b"key-B"), ("body-B", b"key-B"), ("body-A", b"key-A")],
        ),
        (
            [
                KafkaPublishMessage("body-A", key=b"key-A"),
                KafkaPublishMessage("body-B", key=b"key-B"),
                KafkaPublishMessage("body-B", key=b"key-B"),
            ],
            do_nothing,
            [("body-A", b"key-A"), ("body-B", b"key-B"), ("body-B", b"key-B")],
        ),
    ),
)
def test_keys_order(kafka_messages, changing_pattern, expected) -> None:

    cmd = KafkaPublishCommand(
        *kafka_messages,
        topic="topic",
        _publish_type=PublishType.PUBLISH,
    )
    cmd.batch_bodies = changing_pattern(cmd.batch_bodies)
    delivered_cmd = delivered(cmd)
    assert delivered_cmd == expected
