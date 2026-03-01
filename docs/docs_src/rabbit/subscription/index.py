from faststream import FastStream
from faststream.rabbit import RabbitBroker

broker = RabbitBroker()
app = FastStream(broker)


@broker.subscriber("routing_key")  # handle messages by routing key
async def handle(msg):
    print(msg)


@app.after_startup
async def test_publish():
    await broker.publish(
        "message",
        "routing_key",  # publish message with routing key
    )
