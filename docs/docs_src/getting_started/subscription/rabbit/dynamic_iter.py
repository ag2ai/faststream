from faststream.rabbit import RabbitBroker, RabbitMessage

async def main():
    async with RabbitBroker() as broker:
        subscriber = broker.subscriber("test-queue")
        await subscriber.start()

        async for msg in subscriber: # msg is RabbitMessage type
            ... # do message process

        await subscriber.stop()
