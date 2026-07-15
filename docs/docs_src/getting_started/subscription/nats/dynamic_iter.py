from faststream.nats import NatsBroker

async def main():
    async with NatsBroker() as broker:
        subscriber = broker.subscriber("test-subject", persistent=False)

        async with subscriber:
            async for msg in subscriber: # msg is NatsMessage type
                ... # do message process
