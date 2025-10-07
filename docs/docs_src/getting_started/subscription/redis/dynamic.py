from faststream.redis import RedisBroker, RedisChannelMessage

async def main():
    async with RedisBroker() as broker:  # connect the broker
        subscriber = broker.subscriber("test-channel", persistent=False)
        await subscriber.start()

        message: RedisChannelMessage | None = await subscriber.get_one(timeout=3.0)

        await subscriber.stop()

    return message
