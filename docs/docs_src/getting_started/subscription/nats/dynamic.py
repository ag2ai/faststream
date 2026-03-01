from faststream.nats import NatsBroker, NatsMessage

async def main():
    async with NatsBroker() as broker:  # connect the broker
        subscriber = broker.subscriber("test-subject", persistent=False)
        await subscriber.start()

        message: NatsMessage | None = await subscriber.get_one(timeout=3.0)

        await subscriber.stop()

    return message
