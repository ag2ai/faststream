import asyncio
from typing import Any, Optional

from typing_extensions import TypeAlias

from faststream.broker.handler import AsyncHandler
from faststream.sqs.shared.schemas import SQSQueue
from faststream.types import AnyDict

QueueUrl: TypeAlias = str


class LogicSQSHandler(AsyncHandler[AnyDict]):
    queue: SQSQueue
    consumer_params: AnyDict
    task: Optional["asyncio.Task[Any]"] = None

    async def start(self) -> None:
        # TODO
        pass

    async def close(self) -> None:
        # TODO
        pass
