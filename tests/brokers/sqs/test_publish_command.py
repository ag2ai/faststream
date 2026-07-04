import pytest

from faststream.response import ensure_response
from faststream.sqs.response import (
    SQSBatchPublishCommand,
    SQSPublishCommand,
    SQSResponse,
)
from tests.brokers.base.publish_command import BatchPublishCommandTestcase


@pytest.mark.sqs()
class TestPublishCommand(BatchPublishCommandTestcase):
    publish_command_cls = SQSBatchPublishCommand

    def test_sqs_response_class(self) -> None:
        response = ensure_response(
            SQSResponse(
                body=1,
                headers={"1": "1"},
                group_id="g",
                deduplication_id="d",
                delay_seconds=3,
            ),
        )
        cmd = SQSPublishCommand.from_cmd(response.as_publish_command())
        assert cmd.body == 1
        assert cmd.headers == {"1": "1"}
        assert cmd.group_id == "g"
        assert cmd.deduplication_id == "d"
        assert cmd.delay_seconds == 3
