from faststream.mqtt import MQTTBroker, MQTTMessage

async def main():
    async with MQTTBroker() as broker:  # connect the broker
        subscriber = broker.subscriber("test-topic", persistent=False)
        await subscriber.start()

        message: MQTTMessage | None = await subscriber.get_one(timeout=3.0)

        await subscriber.stop()

    return message
