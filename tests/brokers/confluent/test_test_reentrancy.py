from uuid import uuid4

import pytest

from faststream.confluent import KafkaBroker, TestKafkaBroker, TopicPartition

broker = KafkaBroker()


first_topic_name = str(uuid4())
out_topic_name = str(uuid4())

to_output_data = broker.publisher(out_topic_name, partition=0)


@to_output_data
@broker.subscriber(
    partitions=[TopicPartition(first_topic_name, 0)],
    auto_offset_reset="earliest",
)
async def on_input_data(msg: int) -> int:
    return msg + 1


@broker.subscriber(
    partitions=[TopicPartition(out_topic_name, 0)],
    auto_offset_reset="earliest",
)
async def on_output_data(msg: int) -> None:
    pass


async def _test_with_broker(with_real: bool) -> None:
    async with TestKafkaBroker(broker, with_real=with_real) as tester:
        await tester.publish(1, first_topic_name)

        await on_output_data.wait_call(20)

        on_input_data.mock.assert_called_once_with(1)
        to_output_data.mock.assert_called_once_with(2)
        on_output_data.mock.assert_called_once_with(2)


@pytest.mark.asyncio()
@pytest.mark.confluent()
async def test_with_fake_broker() -> None:
    await _test_with_broker(False)
    await _test_with_broker(False)


async def _test_with_temp_subscriber() -> None:
    @broker.subscriber(
        partitions=[TopicPartition(out_topic_name, 0)],
        auto_offset_reset="earliest",
    )
    async def on_output_data(msg: int) -> None:
        pass

    async with TestKafkaBroker(broker) as tester:
        await tester.publish(1, first_topic_name)

        await on_output_data.wait_call(20)

        on_input_data.mock.assert_called_once_with(1)
        to_output_data.mock.assert_called_once_with(2)
        on_output_data.mock.assert_called_once_with(2)


@pytest.mark.asyncio()
@pytest.mark.confluent()
async def test_with_temp_subscriber() -> None:
    await _test_with_temp_subscriber()
    await _test_with_temp_subscriber()
