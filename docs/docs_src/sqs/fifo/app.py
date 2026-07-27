import pytest

from faststream import FastStream
from faststream.sqs import FifoQueue, SQSBroker, TestSQSBroker

broker = SQSBroker(region_name="us-east-1")
app = FastStream(broker)

orders = FifoQueue(
    name="orders",
    content_based_deduplication=True,
    deduplication_scope="messageGroup",
    fifo_throughput_limit="perMessageGroupId",
)


@broker.subscriber(orders)
async def handler(msg: str) -> None: ...


async def publish() -> None:
    await broker.publish(
        "ordered-event",
        orders,
        group_id="customer-42",
        deduplication_id="evt-1001",
    )


@pytest.mark.asyncio
async def test_fifo() -> None:
    async with TestSQSBroker(broker):
        await broker.publish(
            "ordered-event",
            orders,
            group_id="customer-42",
            deduplication_id="evt-1001",
        )
        handler.mock.assert_called_once_with("ordered-event")
