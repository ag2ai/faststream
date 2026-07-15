from faststream import FastStream
from faststream.mqtt import MQTTBroker

broker = MQTTBroker("localhost", port=1883)

app = FastStream(broker)


@broker.subscriber("test")
async def base_handler(body):
    print(body)
