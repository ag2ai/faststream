from faststream import FastStream
from faststream.mqtt import MQTTBroker, QoS

broker = MQTTBroker("localhost", version="5.0")
app = FastStream(broker)


@broker.subscriber("workers/jobs/#", qos=QoS.AT_LEAST_ONCE, shared="pool-a")
async def handle_job(cmd: dict) -> None:
    print(cmd)
