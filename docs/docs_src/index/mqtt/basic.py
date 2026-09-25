from faststream import FastStream
from faststream.mqtt import MQTTBroker

broker = MQTTBroker("localhost", port=1883)
app = FastStream(broker)

@broker.subscriber("in-topic")
@broker.publisher("out-topic")
async def handle_msg(user: str, user_id: int) -> str:
    return f"User: {user_id} - {user} registered"
