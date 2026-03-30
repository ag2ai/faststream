from faststream import FastStream
from faststream.mq import MQBroker

broker = MQBroker(
    queue_manager="QM1",
    ccdt_url="file:///etc/mq/AMQCLCHL.TAB",
    reconnect="qmgr",
    username="app",
    password="password",
)
app = FastStream(broker)


@broker.subscriber("orders")
async def handle(msg: str) -> str:
    return msg.upper()
