import asyncio
import logging
import time
from typing import Any

from faststream import FastStream
from faststream.rabbit import RabbitBroker

broker = RabbitBroker(log_level=logging.DEBUG)
app = FastStream(broker)

EVENTS_PROCESSED = 0
publisher = broker.publisher("test")
# For confluent-kafka need to add some args for broker:
# auto_offset_reset=earliest, partitions=[TopicPartition("test", 0)]  # noqa: ERA001
# import TopicPartition from faststream.confluent
subscriber = broker.subscriber()


@subscriber
@publisher
async def handle(message: Any) -> Any:
    global EVENTS_PROCESSED  # noqa: PLW0603
    EVENTS_PROCESSED += 1
    return message


# It is better for the benchmark to work for a longer time.
# This will allow you to get more accurate results
async def main() -> None:
    payload = {
        "name": "Jo",
        "age": 39,
        "fullname": "LongString" * 8,
        "nes": {
            "name": "Jo",
            "age": 22,
            "fullname": "LongString" * 8
        }
    }
    await broker.start()
    start_time = time.time()
    await publisher.publish(payload)
    await app.run()
    eps = EVENTS_PROCESSED / (time.time() - start_time)
    app.logger.log(f"Events per second: {eps}", logging.INFO)


if __name__ == "__main__":
    asyncio.run(main())
