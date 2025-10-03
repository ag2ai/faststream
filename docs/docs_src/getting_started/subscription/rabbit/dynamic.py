from faststream.rabbit import RabbitBroker, RabbitMessage

async def main():
    async with RabbitBroker() as broker:  # connect the broker
        subscriber = broker.subscriber("test-queue", persistent=False)
        await subscriber.start()

        message: RabbitMessage | None = await subscriber.get_one(timeout=3.0)

        await subscriber.stop()

    return message
