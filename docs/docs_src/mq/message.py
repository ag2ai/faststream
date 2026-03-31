from faststream import Context
from faststream.mq import MQBroker, MQMessage

broker = MQBroker(queue_manager="QM1")


@broker.subscriber("orders.incoming")
async def handle(
    body: dict,
    msg: MQMessage,
    correlation_id: str | None = Context("message.correlation_id"),
) -> None:
    print(body)
    print(msg.raw_message.queue)
    print(correlation_id)
