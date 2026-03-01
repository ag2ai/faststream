---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# In-Progress sender

Nats Jetstream uses the at least once principle, so the message will be delivered until it receives the ACK status (even if your handler takes a long time to process the message), so you can extend the message processing status with a request

??? example "Full Example"
    ```python linenums="1"
    import asyncio

    from faststream import Depends, FastStream
    from faststream.nats import NatsBroker, NatsMessage

    broker = NatsBroker()
    app = FastStream(broker)

    async def progress_sender(message: NatsMessage):
        async def in_progress_task():
            while True:
                await asyncio.sleep(10.0)
                await message.in_progress()

        task = asyncio.create_task(in_progress_task())
        yield
        task.cancel()

    @broker.subscriber("test", dependencies=[Depends(progress_sender)])
    async def handler():
        await asyncio.sleep(20.0)

    ```
