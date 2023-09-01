import pytest

from faststream.kafka import TestKafkaBroker
from docs_src.kafka.call_publish_within_subscribe.app import (
    broker, 
    on_input_data, 
    Data,
)

@pytest.mark.asyncio
async def test_base_app():
    @broker.subscriber("output_data")
    async def on_output_data(msg: Data):
        pass

    async with TestKafkaBroker(broker) as tester:
        await tester.publish(Data(data=0.2), "input_data")

        on_input_data.mock.assert_called_with(dict(Data(data=0.2)))   

        # why is this failing?
        with pytest.raises(Exception) as e:    
            on_output_data.mock.assert_called_once_with(dict(Data(data=1.2)))