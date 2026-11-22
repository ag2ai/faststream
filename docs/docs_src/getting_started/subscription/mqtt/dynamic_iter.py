from faststream.mqtt import MQTTBroker, MQTTMessage

async def main():
    async with MQTTBroker() as broker:
        subscriber = broker.subscriber("dynamic-topic", persistent=False)
        await subscriber.start()

        async for msg in subscriber: # msg is MQTTMessage type
            ... # do message process

        await subscriber.stop()
