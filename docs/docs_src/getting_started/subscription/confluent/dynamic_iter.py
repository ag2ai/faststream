from faststream.confluent import KafkaBroker, KafkaMessage

async def main():
    async with KafkaBroker() as broker:
        subscriber = broker.subscriber("test-topic", persistent=False)
        await subscriber.start()

        async for msg in subscriber: # msg is KafkaMessage type
            ... # do message process

        await subscriber.stop()
