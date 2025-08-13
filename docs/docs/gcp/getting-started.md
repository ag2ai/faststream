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
from faststream.gcppubsub import GCPBroker

# Create broker with project ID
broker = GCPBroker(project_id="your-gcp-project-id")
```

### Subscribing to Messages

```python
from faststream import FastStream, Logger
from faststream.gcppubsub import GCPBroker

broker = GCPBroker(project_id="your-project")
app = FastStream(broker)

@broker.subscriber("my-subscription", topic="my-topic")
async def handle_message(msg: str, logger: Logger):
    logger.info(msg)
```

### Publishing Messages

```python
from faststream import FastStream, Logger
from faststream.gcppubsub import GCPBroker

broker = GCPBroker(project_id="your-project")
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
from faststream.gcppubsub import GCPBroker

broker = GCPBroker(
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
from faststream.gcppubsub import GCPMessage

@broker.subscriber("my-subscription", topic="my-topic")
async def handle_message(message: GCPMessage):
    # Access message data
    print(f"Data: {message.body}")
    print(f"Message ID: {message.message_id}")
    print(f"Attributes: {message.attributes}")
    print(f"Publish time: {message.publish_time}")
```

### Manual Acknowledgment

```python
@broker.subscriber("my-subscription", topic="my-topic")
async def handle_message(message: GCPMessage):
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
from faststream.gcppubsub import GCPBroker

# Using service account JSON file
broker = GCPBroker(
    project_id="your-project",
    service_file="/path/to/credentials.json"
)

# Using environment variable
# Set GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
broker = GCPBroker(project_id="your-project")
```

### Using Default Credentials

If running on Google Cloud (GCE, GKE, Cloud Run, etc.), default credentials will be used automatically:

```python
broker = GCPBroker(project_id="your-project")
```

## Environment Variables

You can configure the GCP Pub/Sub broker using environment variables, which is especially useful for deployment and testing scenarios.

### Authentication Environment Variables

```bash
# Google Cloud credentials
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/credentials.json"

# Or using service account key content (for containerized environments)
export GOOGLE_APPLICATION_CREDENTIALS_JSON='{"type": "service_account", ...}'
```

### Project Configuration

```bash
# GCP Project ID
export GCP_PROJECT_ID="your-project-id"
export GOOGLE_CLOUD_PROJECT="your-project-id"  # Alternative name
```

### Pub/Sub Emulator (for local development)

```bash
# Enable emulator mode
export PUBSUB_EMULATOR_HOST="localhost:8681"
export GCP_PROJECT_ID="test-project"
```

### Example with Environment Variables

```python
import os
from faststream.gcppubsub import GCPBroker

# Read project ID from environment variable
project_id = os.getenv("GCP_PROJECT_ID", "default-project")

# Use emulator if environment variable is set
emulator_host = os.getenv("PUBSUB_EMULATOR_HOST")

broker = GCPBroker(
    project_id=project_id,
    emulator_host=emulator_host,  # Will be None if not set
)
```

### Docker Environment Variables

When running in Docker, you can pass environment variables:

```dockerfile
# In your Dockerfile
ENV GCP_PROJECT_ID=your-project
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json

# Copy credentials file
COPY credentials.json /app/credentials.json
```

Or using docker-compose:

```yaml
# docker-compose.yml
version: '3.8'
services:
  app:
    build: .
    environment:
      - GCP_PROJECT_ID=your-project
      - GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json
    volumes:
      - ./credentials.json:/app/credentials.json:ro
```

### Configuration with Environment Variables

You can also use environment variables for broker configuration:

```python
import os
from faststream.gcppubsub import GCPBroker, PublisherConfig, SubscriberConfig

broker = GCPBroker(
    project_id=os.getenv("GCP_PROJECT_ID"),
    emulator_host=os.getenv("PUBSUB_EMULATOR_HOST"),
    publisher_config=PublisherConfig(
        max_messages=int(os.getenv("PUBSUB_PUBLISHER_MAX_MESSAGES", "100")),
        max_bytes=int(os.getenv("PUBSUB_PUBLISHER_MAX_BYTES", "1048576")),
    ),
    subscriber_config=SubscriberConfig(
        max_messages=int(os.getenv("PUBSUB_SUBSCRIBER_MAX_MESSAGES", "1000")),
        ack_deadline=int(os.getenv("PUBSUB_ACK_DEADLINE", "600")),
    ),
)
```

## Next Steps

- [Message Patterns](./patterns.md) - Learn about different messaging patterns
- [Testing](./testing.md) - How to test your GCP Pub/Sub applications
- [Advanced Configuration](./advanced.md) - Advanced broker and message configuration
