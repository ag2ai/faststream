import pytest

from faststream.rabbit import RabbitBroker, TestRabbitBroker

broker = RabbitBroker()


to_output_data = broker.publisher("output_data")


@to_output_data
@broker.subscriber("input_data")
async def on_input_data(msg: int):
    return msg + 1


@broker.subscriber("output_data")
async def on_output_data(msg: int):
    pass


async def _test_with_broker(with_real: bool):
    async with TestRabbitBroker(broker, with_real=with_real) as tester:
        await tester.publish(1, "input_data")

        await on_output_data.wait_call(3)

        on_input_data.mock.assert_called_with(1)
        to_output_data.mock.assert_called_with(2)
        on_output_data.mock.assert_called_once_with(2)


@pytest.mark.asyncio
async def test_with_fake_broker():
    await _test_with_broker(False)
    await _test_with_broker(False)


@pytest.mark.asyncio
@pytest.mark.rabbit
async def test_with_real_broker():
    await _test_with_broker(True)
    await _test_with_broker(True)


async def _test_with_temp_subscriber():
    @broker.subscriber("output_data")
    async def on_output_data(msg: int):
        pass

    async with TestRabbitBroker(broker) as tester:
        await tester.publish(1, "input_data")

        await on_output_data.wait_call(3)

        on_input_data.mock.assert_called_with(1)
        to_output_data.mock.assert_called_with(2)
        on_output_data.mock.assert_called_once_with(2)


@pytest.mark.asyncio
@pytest.mark.skip(
    reason=(
        "Failed due `on_output_data` subscriber creates inside test and doesn't removed after "
        "https://github.com/ag2ai/faststream/issues/556"
    )
)
async def test_with_temp_subscriber():
    await _test_with_temp_subscriber()
    await _test_with_temp_subscriber()
