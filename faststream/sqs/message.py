from typing import Any

from faststream.broker.message import StreamMessage
from faststream.types import AnyDict


class SQSMessage(StreamMessage[AnyDict]):
    def __init__(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.commited = False

    async def ack(self, **kwargs: Any) -> None:
        # TODO: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
        self.commited = True

    async def nack(self, **kwargs: Any) -> None:
        # TODO
        self.commited = True

    async def reject(self, **kwargs: Any) -> None:
        # TODO
        self.commited = True
