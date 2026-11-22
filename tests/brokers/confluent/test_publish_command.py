import pytest

from faststream.confluent.response import KafkaPublishCommand, KafkaResponse
from faststream.response import ensure_response
from faststream.response.publish_type import PublishType
from tests.brokers.base.publish_command import BatchPublishCommandTestcase


@pytest.mark.confluent()
class TestPublishCommand(BatchPublishCommandTestcase):
    publish_command_cls = KafkaPublishCommand

    @pytest.mark.parametrize("body", (None, "duplicate"))
    def test_duplicate_bodies_keep_distinct_keys_after_reassignment(
        self, body: str | None
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3359."""
        cmd = self.publish_command_cls(
            "present",
            KafkaResponse(body, key=b"key-a"),
            KafkaResponse(body, key=b"key-b"),
            topic="events",
            key=b"key-default",
            _publish_type=PublishType.PUBLISH,
        )
        expected_keys = (b"key-default", b"key-a", b"key-b")
        assert tuple(cmd.key_for(i) for i in range(3)) == expected_keys

        cmd.batch_bodies = cmd.batch_bodies

        assert cmd.batch_bodies == ("present", body, body)
        assert tuple(cmd.key_for(i) for i in range(3)) == expected_keys

    def test_kafka_response_class(self) -> None:
        response = ensure_response(KafkaResponse(body=1, headers={"1": 1}, key=b"1"))
        cmd = self.publish_command_cls.from_cmd(response.as_publish_command())
        assert cmd.body == 1
        assert cmd.headers == {"1": 1}
        assert cmd.key == b"1"
