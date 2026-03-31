from faststream.mq import MQBroker

broker = MQBroker(queue_manager="QM1")


async def publish() -> None:
    async with broker:
        await broker.publish(
            {"message": "hello"},
            queue="orders.incoming",
            headers={"source": "docs"},
        )
