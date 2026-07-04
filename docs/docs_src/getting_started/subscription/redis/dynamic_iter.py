from faststream.redis import RedisBroker

async def main():
    async with RedisBroker() as broker:
        subscriber = broker.subscriber("test-channel", persistent=False)

        async with subscriber:
            async for msg in subscriber: # msg is RedisMessage type
                ... # do message process
