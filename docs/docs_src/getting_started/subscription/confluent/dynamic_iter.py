from faststream.confluent import KafkaBroker

async def main():
    async with KafkaBroker() as broker:
        subscriber = broker.subscriber("dynamic-topic-confluent", persistent=False)

        async with subscriber:
            async for msg in subscriber: # msg is KafkaMessage type
                ... # do message process
