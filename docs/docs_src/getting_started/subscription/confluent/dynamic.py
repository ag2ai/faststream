from faststream.confluent import KafkaBroker, KafkaMessage

async def main():
    async with KafkaBroker() as broker:  # connect the broker
        subscriber = broker.subscriber("test-topic", persistent=False)
        await subscriber.start()

        message: KafkaMessage | None = await subscriber.get_one(timeout=3.0)

        await subscriber.stop()

        async with subscriber:
            message: KafkaMessage | None = await subscriber.get_one(timeout=3.0)

    return message
