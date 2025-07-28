import pytest

from faststream.nats import NatsBroker, TestNatsBroker

broker = NatsBroker()


to_output_data = broker.publisher("output_data")


@to_output_data
@broker.subscriber("input_data")
async def on_input_data(msg: int) -> int:
    return msg + 1


@broker.subscriber("output_data")
async def on_output_data(msg: int) -> None:
    pass


async def _test_with_broker(with_real: bool) -> None:
    async with TestNatsBroker(broker, with_real=with_real) as tester:
        await tester.publish(1, "input_data")

        await on_output_data.wait_call(3)

        on_input_data.mock.assert_called_once_with(1)
        to_output_data.mock.assert_called_once_with(2)
        on_output_data.mock.assert_called_once_with(2)


@pytest.mark.asyncio()
@pytest.mark.nats()
async def test_with_fake_broker() -> None:
    await _test_with_broker(False)
    await _test_with_broker(False)


@pytest.mark.asyncio()
@pytest.mark.nats()
@pytest.mark.connected()
async def test_with_real_broker() -> None:
    await _test_with_broker(True)
    await _test_with_broker(True)


async def _test_with_temp_subscriber() -> None:
    @broker.subscriber("output_data")
    async def on_output_data(msg: int) -> None:
        pass

    async with TestNatsBroker(broker) as tester:
        await tester.publish(1, "input_data")

        await on_output_data.wait_call(3)

        on_input_data.mock.assert_called_once_with(1)
        to_output_data.mock.assert_called_once_with(2)
        on_output_data.mock.assert_called_once_with(2)


@pytest.mark.asyncio()
@pytest.mark.nats()
async def test_with_temp_subscriber() -> None:
    await _test_with_temp_subscriber()
    await _test_with_temp_subscriber()
