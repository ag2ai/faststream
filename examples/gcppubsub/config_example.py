"""Example showing grouped configuration usage."""

import os
from faststream import FastStream, Logger
from faststream.gcppubsub import (
    GCPPubSubBroker,
    PublisherConfig,
    RetryConfig,
    SubscriberConfig,
)

# Create configuration objects with environment variable defaults
publisher_config = PublisherConfig(
    max_messages=int(os.getenv("PUBSUB_PUBLISHER_MAX_MESSAGES", "50")),
    max_bytes=int(os.getenv("PUBSUB_PUBLISHER_MAX_BYTES", "512000")),  # 512KB
    max_latency=float(os.getenv("PUBSUB_PUBLISHER_MAX_LATENCY", "0.05")),  # 50ms
)

subscriber_config = SubscriberConfig(
    max_messages=int(os.getenv("PUBSUB_SUBSCRIBER_MAX_MESSAGES", "100")),
    ack_deadline=int(os.getenv("PUBSUB_ACK_DEADLINE", "300")),  # 5 minutes
    max_extension=int(os.getenv("PUBSUB_MAX_EXTENSION", "300")),  # 5 minutes
)

retry_config = RetryConfig(
    max_attempts=int(os.getenv("PUBSUB_RETRY_MAX_ATTEMPTS", "3")),
    max_delay=float(os.getenv("PUBSUB_RETRY_MAX_DELAY", "30.0")),
    multiplier=float(os.getenv("PUBSUB_RETRY_MULTIPLIER", "1.5")),
    min_delay=float(os.getenv("PUBSUB_RETRY_MIN_DELAY", "0.5")),
)

# Create broker with grouped configuration
broker = GCPPubSubBroker(
    project_id=os.getenv("GCP_PROJECT_ID", "test-project"),
    emulator_host=os.getenv("PUBSUB_EMULATOR_HOST"),
    publisher_config=publisher_config,
    subscriber_config=subscriber_config,
    retry_config=retry_config,
)

app = FastStream(broker)


@broker.subscriber("config-subscription", topic="config-topic")
async def handle_config_message(msg: str, logger: Logger):
    logger.info(f"Received configured message: {msg}")


@app.after_startup
async def send_test_message():
    await broker.publish("Configuration test message!", "config-topic")


if __name__ == "__main__":
    import uvloop
    import asyncio

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    app.run()
