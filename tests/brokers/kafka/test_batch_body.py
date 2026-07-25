import pytest

from faststream.kafka.response import KafkaPublishCommand, KafkaPublishMessage
from faststream.response.publish_type import PublishType

body_a = "body-A"
body_b = "body-B"
body_c = "body-C"


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
        (
            [
                KafkaPublishMessage(body_a, key=b"key-1"),
                KafkaPublishMessage(body_c, key=b"key-2"),
                KafkaPublishMessage(body_b, key=b"key-3"),
                KafkaPublishMessage(body_a, key=b"key-4"),
                KafkaPublishMessage(body_b, key=b"key-5"),
                KafkaPublishMessage(body_a, key=b"key-6"),
                KafkaPublishMessage(body_a, key=b"key-7"),
                KafkaPublishMessage(body_b, key=b"key-8"),
            ],
            do_nothing,
            [
                ("body-A", b"key-1"),
                ("body-C", b"key-2"),
                ("body-B", b"key-3"),
                ("body-A", b"key-4"),
                ("body-B", b"key-5"),
                ("body-A", b"key-6"),
                ("body-A", b"key-7"),
                ("body-B", b"key-8"),
            ],
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


@pytest.mark.kafka()
@pytest.mark.parametrize(
    ("kafka_messages", "changing_pattern", "expected"),
    (
        (
            [
                KafkaPublishMessage(body_a, key=b"key-1"),
                KafkaPublishMessage(body_c, key=b"key-2"),
                KafkaPublishMessage(body_b, key=b"key-3"),
                KafkaPublishMessage(body_a, key=b"key-4"),
                KafkaPublishMessage(body_b, key=b"key-5"),
                KafkaPublishMessage(body_a, key=b"key-6"),
                KafkaPublishMessage(body_a, key=b"key-7"),
                KafkaPublishMessage(body_b, key=b"key-8"),
            ],
            reverse_bodies,
            [
                ("body-A", b"key-1"),
                ("body-C", b"key-2"),
                ("body-B", b"key-3"),
                ("body-A", b"key-4"),
                ("body-B", b"key-5"),
                ("body-A", b"key-6"),
                ("body-A", b"key-7"),
                ("body-B", b"key-8"),
            ],
        ),
        pytest.param(
            [
                KafkaPublishMessage(body_a, key=b"key-1"),
                KafkaPublishMessage(body_c, key=b"key-2"),
                KafkaPublishMessage(body_b, key=b"key-3"),
                KafkaPublishMessage(body_a, key=b"key-4"),
                KafkaPublishMessage(body_b, key=b"key-5"),
                KafkaPublishMessage(body_a, key=b"key-6"),
                KafkaPublishMessage(body_a, key=b"key-7"),
                KafkaPublishMessage(body_b, key=b"key-8"),
            ],
            remove_body,
            [
                ("body-C", b"key-2"),
                ("body-B", b"key-3"),
                ("body-A", b"key-4"),
                ("body-B", b"key-5"),
                ("body-A", b"key-6"),
                ("body-A", b"key-7"),
                ("body-B", b"key-8"),
            ],
            marks=pytest.mark.xfail(
                reason="It is not possible to track the relationship between identical bodies and keys after removing bodies, as the keys are not unique."
            ),
        ),
    ),
)
def test_random_bodies(kafka_messages, changing_pattern, expected) -> None:

    cmd = KafkaPublishCommand(
        *kafka_messages,
        topic="topic",
        _publish_type=PublishType.PUBLISH,
    )
    cmd.batch_bodies = changing_pattern(cmd.batch_bodies)
    delivered_cmd = delivered(cmd)
    for body in expected:
        assert body in delivered_cmd
