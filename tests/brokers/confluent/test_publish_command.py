from faststream.confluent.response import KafkaPublishCommand, KafkaResponse
from faststream.response import ensure_response
from tests.brokers.base.publish_command import BatchPublishCommandTestcase


class TestPublishCommand(BatchPublishCommandTestcase):
    publish_command_cls = KafkaPublishCommand

    def test_kafka_response_class(self) -> None:
        response = ensure_response(KafkaResponse(body=1, headers={"1": 1}, key=b"1"))
        cmd = self.publish_command_cls.from_cmd(response.as_publish_command())
        assert cmd.body == 1
        assert cmd.headers == {"1": 1}
        assert cmd.key == b"1"
