from faststream.rabbit import RabbitBroker

async def main():
    async with RabbitBroker() as broker:
        subscriber = broker.subscriber("test-queue", persistent=False)

        async with subscriber:
            async for msg in subscriber: # msg is RabbitMessage type
                ... # do message process
