# Getting Started with GCP Pub/Sub

FastStream supports Google Cloud Pub/Sub as a broker for building asynchronous applications. This guide will help you get started with GCP Pub/Sub in FastStream.

## Installation

To use GCP Pub/Sub with FastStream, install the extra dependencies:

```bash
pip install "faststream[gcppubsub]"
```

This installs the required dependencies:
- `gcloud-aio-pubsub` - Async GCP Pub/Sub client
- `aiohttp` - HTTP client for async operations
- `backoff` - Exponential backoff for retries

## Basic Usage

### Creating a Broker

```python
from faststream.gcppubsub import GCPPubSubBroker

# Create broker with project ID
broker = GCPPubSubBroker(project_id="your-gcp-project-id")
```

### Subscribing to Messages

```python
from faststream import FastStream, Logger
from faststream.gcppubsub import GCPPubSubBroker

broker = GCPPubSubBroker(project_id="your-project")
app = FastStream(broker)

@broker.subscriber("my-subscription", topic="my-topic")
async def handle_message(msg: str, logger: Logger):
    logger.info(msg)
```

### Publishing Messages

```python
from faststream import FastStream, Logger
from faststream.gcppubsub import GCPPubSubBroker

broker = GCPPubSubBroker(project_id="your-project")
app = FastStream(broker)

@broker.subscriber("my-subscription", topic="my-topic")
async def handle_message(msg: str, logger: Logger):
    logger.info(msg)

@app.after_startup
async def publish_message():
    await broker.publish("Hello World!", "my-topic")
```

## Configuration

### Broker Configuration

```python
from faststream.gcppubsub import GCPPubSubBroker

broker = GCPPubSubBroker(
    project_id="your-project",
    # Optional: specify service account credentials
    service_account_path="/path/to/credentials.json",
    # Optional: set default ack deadline
    subscriber_ack_deadline=60,
    # Optional: enable message ordering
    enable_message_ordering=True,
)
```

### Subscriber Configuration

```python
@broker.subscriber(
    "my-subscription",
    topic="my-topic",
    # Create subscription if it doesn't exist
    create_subscription=True,
    # ACK deadline in seconds
    ack_deadline=30,
    # Max messages to pull at once
    max_messages=10,
)
async def handle_message(message: str):
    # Process message
    return "processed"
```

### Publisher Configuration

```python
@broker.publisher(
    "my-topic",
    # Create topic if it doesn't exist
    create_topic=True,
    # Optional: message ordering key
    ordering_key="user-123",
)
async def publish_update(data: dict) -> dict:
    return data
```

## Message Handling

### Message Structure

GCP Pub/Sub messages in FastStream have the following structure:

```python
from faststream.gcppubsub import GCPPubSubMessage

@broker.subscriber("my-subscription", topic="my-topic")
async def handle_message(message: GCPPubSubMessage):
    # Access message data
    print(f"Data: {message.body}")
    print(f"Message ID: {message.message_id}")
    print(f"Attributes: {message.attributes}")
    print(f"Publish time: {message.publish_time}")
```

### Manual Acknowledgment

```python
@broker.subscriber("my-subscription", topic="my-topic")
async def handle_message(message: GCPPubSubMessage):
    try:
        # Process message
        await process_message(message.body)
        # Message is automatically acknowledged on success
    except Exception as e:
        # Message will be negatively acknowledged
        # and redelivered based on subscription settings
        raise e
```

## Authentication

### Using Service Account

```python
from faststream.gcppubsub import GCPPubSubBroker

# Using service account JSON file
broker = GCPPubSubBroker(
    project_id="your-project",
    service_account_path="/path/to/credentials.json"
)

# Using environment variable
# Set GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
broker = GCPPubSubBroker(project_id="your-project")
```

### Using Default Credentials

If running on Google Cloud (GCE, GKE, Cloud Run, etc.), default credentials will be used automatically:

```python
broker = GCPPubSubBroker(project_id="your-project")
```

## Next Steps

- [Message Patterns](./patterns.md) - Learn about different messaging patterns
- [Testing](./testing.md) - How to test your GCP Pub/Sub applications
- [Advanced Configuration](./advanced.md) - Advanced broker and message configuration
