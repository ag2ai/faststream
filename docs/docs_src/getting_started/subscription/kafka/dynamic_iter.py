from faststream.kafka import KafkaBroker

async def main():
    async with KafkaBroker() as broker:
        subscriber = broker.subscriber("dynamic-topic", persistent=False)

        async with subscriber:
            async for msg in subscriber: # msg is KafkaMessage type
                ... # do message process
