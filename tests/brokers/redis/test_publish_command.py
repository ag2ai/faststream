from faststream.redis.response import RedisPublishCommand, RedisResponse
from faststream.response import ensure_response
from tests.brokers.base.publish_command import BatchPublishCommandTestcase


class TestPublishCommand(BatchPublishCommandTestcase):
    publish_command_cls = RedisPublishCommand

    def test_redis_response_class(self) -> None:
        response = ensure_response(RedisResponse(body=1, headers={"1": 1}, maxlen=1))
        cmd = self.publish_command_cls.from_cmd(response.as_publish_command())
        assert cmd.body == 1
        assert cmd.headers == {"1": 1}
        assert cmd.maxlen == 1
