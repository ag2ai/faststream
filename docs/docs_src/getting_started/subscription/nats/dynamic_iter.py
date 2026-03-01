from faststream.nats import NatsBroker, NatsMessage

async def main():
    async with NatsBroker() as broker:
        subscriber = broker.subscriber("test-subject", persistent=False)
        await subscriber.start()

        async for msg in subscriber: # msg is NatsMessage type
            ... # do message process

        await subscriber.stop()
