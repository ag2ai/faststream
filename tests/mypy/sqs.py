from typing_extensions import assert_type

from faststream.sqs import SQSBroker, SQSMessage, TestSQSBroker


async def check_multiple_test_brokers() -> None:
    async with TestSQSBroker(SQSBroker()) as br1:
        assert_type(br1, SQSBroker)
        await br1.publish(None, "test")

    async with TestSQSBroker(
        SQSBroker(),
        SQSBroker(),
    ) as (br1, br2):
        await br1.publish(None, "test")
        await br2.publish(None, "test")


async def check_request_response_type() -> None:
    broker = SQSBroker()

    broker_response = await broker.request(None, "test")
    assert_type(broker_response, SQSMessage)
