from faststream.redis import RedisBroker, RedisMessage

async def main():
    async with RedisBroker() as broker:
        subscriber = broker.subscriber("test-channel", persistent=False)
        await subscriber.start()

        async for msg in subscriber: # msg is RedisMessage type
            ... # do message process

        await subscriber.stop()
