---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Message Acknowledgment in GCP Pub/Sub

Message acknowledgment is crucial for reliable message processing in Google Cloud Pub/Sub. FastStream provides flexible acknowledgment strategies to ensure messages are processed exactly as needed.

## Understanding Acknowledgment

In Pub/Sub, acknowledgment (ack) confirms that a message has been successfully processed:

- **Acknowledged messages** are removed from the subscription
- **Unacknowledged messages** are redelivered after the ack deadline
- **Negatively acknowledged messages** (nack) are immediately available for redelivery

## Automatic Acknowledgment

By default, FastStream automatically acknowledges messages after successful processing:

```python
from faststream.gcp import GCPBroker

broker = GCPBroker(project_id="your-project-id")

@broker.subscriber("auto-ack-sub", topic="events")
async def handle_auto_ack(msg: dict):
    # Message is automatically acknowledged if this function completes successfully
    await process_message(msg)
    # Ack happens here automatically

    # If an exception is raised, the message is NOT acknowledged
    # and will be redelivered after the ack deadline
```

### Auto-ack Behavior

- **Success**: Message is acknowledged automatically
- **Exception**: Message is not acknowledged, will be redelivered
- **Return value**: Message is acknowledged regardless of return value

## Manual Acknowledgment

For fine-grained control, disable auto-acknowledgment:

```python
@broker.subscriber(
    "manual-ack-sub",
    topic="important-events",
    auto_ack=False  # Disable automatic acknowledgment
)
async def handle_manual_ack(msg: dict, message: NativeMessage):
    try:
        # Process the message
        result = await process_important_message(msg)

        if result.success:
            # Manually acknowledge on success
            await message.ack()
            logger.info("Message processed and acknowledged")
        else:
            # Don't acknowledge - message will be redelivered
            logger.warning("Processing incomplete, message will retry")

    except CriticalError as e:
        # Negative acknowledgment - immediate redelivery
        await message.nack()
        logger.error(f"Critical error, immediate retry: {e}")

    except Exception as e:
        # Let message timeout for redelivery with backoff
        logger.error(f"Error processing, will retry after deadline: {e}")
```

## Acknowledgment Deadline

Configure how long Pub/Sub waits before redelivering unacknowledged messages:

```python
from faststream.gcp import SubscriberConfig

@broker.subscriber(
    "deadline-sub",
    topic="slow-processing",
    config=SubscriberConfig(
        ack_deadline_seconds=600  # 10 minutes to process
    )
)
async def handle_slow_process(msg: dict):
    # You have 10 minutes to process this message
    await long_running_operation(msg)
    # Auto-ack happens after successful completion
```

### Deadline Extension

For very long processing, extend the deadline:

```python
@broker.subscriber(
    "extend-deadline-sub",
    topic="very-slow-processing",
    auto_ack=False,
    config=SubscriberConfig(
        ack_deadline_seconds=60  # Initial 1 minute
    )
)
async def handle_with_extension(msg: dict, message: NativeMessage):
    # Start processing
    for step in range(10):
        # Extend deadline before it expires
        await message.modify_ack_deadline(60)  # Extend by another minute

        # Do work
        await process_step(step, msg)

    # Acknowledge when done
    await message.ack()
```

## Negative Acknowledgment (Nack)

Explicitly reject messages for immediate redelivery:

```python
@broker.subscriber(
    "nack-sub",
    topic="validation-required",
    auto_ack=False
)
async def handle_with_validation(msg: dict, message: NativeMessage):
    # Validate message
    if not validate_message(msg):
        # Immediately redelivery for retry
        await message.nack()
        logger.warning("Invalid message, nacking for immediate retry")
        return

    try:
        await process_valid_message(msg)
        await message.ack()

    except TemporaryError:
        # Nack for immediate retry
        await message.nack()

    except PermanentError:
        # Ack to prevent infinite retries
        await message.ack()
        await send_to_dead_letter(msg)
```

## Individual Message Processing

Each message is processed individually even when multiple messages are pulled:

```python
@broker.subscriber(
    "individual-sub",
    topic="events",
    config=SubscriberConfig(max_messages=10),  # Pull multiple for efficiency
    auto_ack=False
)
async def handle_individual(msg: dict, message: NativeMessage):
    # This handler processes one message at a time
    try:
        await process_message(msg)
        await message.ack()
    except Exception as e:
        logger.error(f"Failed to process: {e}")
        await message.nack()
```

!!! note
    Even though `max_messages` may pull multiple messages from Pub/Sub, each message is processed individually by your handler function. There is no batch processing where a handler receives multiple messages at once.

## Conditional Acknowledgment

Acknowledge based on processing results:

```python
from enum import Enum

class ProcessingResult(Enum):
    SUCCESS = "success"
    RETRY = "retry"
    SKIP = "skip"
    ERROR = "error"

@broker.subscriber(
    "conditional-sub",
    topic="conditional-events",
    auto_ack=False
)
async def handle_conditional(msg: dict, message: NativeMessage):
    result = await process_with_result(msg)

    if result == ProcessingResult.SUCCESS:
        # Normal acknowledgment
        await message.ack()
        logger.info("Message processed successfully")

    elif result == ProcessingResult.RETRY:
        # Don't ack - will retry after deadline
        logger.info("Message will be retried")

    elif result == ProcessingResult.SKIP:
        # Ack even though not fully processed
        await message.ack()
        logger.warning("Message skipped but acknowledged")

    elif result == ProcessingResult.ERROR:
        # Immediate retry
        await message.nack()
        logger.error("Message errored, immediate retry")
```

## Dead Letter Queue Pattern

Configure dead letter topics for messages that repeatedly fail:

```python
@broker.subscriber(
    "dlq-sub",
    topic="risky-events",
    config=SubscriberConfig(
        dead_letter_policy={
            "dead_letter_topic": "projects/your-project/topics/dlq-topic",
            "max_delivery_attempts": 5
        }
    )
)
async def handle_with_dlq(msg: dict):
    # After 5 failed attempts, message automatically goes to DLQ
    if not is_valid(msg):
        raise ValueError("Invalid message")  # Will retry up to 5 times

    await process_message(msg)
    # Auto-ack on success
```

## Monitoring Acknowledgments

Track acknowledgment metrics:

```python
from dataclasses import dataclass
from datetime import datetime

@dataclass
class AckMetrics:
    total_received: int = 0
    total_acked: int = 0
    total_nacked: int = 0
    total_timeout: int = 0

    @property
    def ack_rate(self):
        if self.total_received == 0:
            return 0
        return self.total_acked / self.total_received

metrics = AckMetrics()

@broker.subscriber("monitored-sub", topic="events", auto_ack=False)
async def handle_monitored(msg: dict, message: NativeMessage):
    metrics.total_received += 1
    start_time = datetime.now()

    try:
        await process_message(msg)
        await message.ack()
        metrics.total_acked += 1

    except TemporaryError:
        await message.nack()
        metrics.total_nacked += 1

    except Exception:
        # Let it timeout
        metrics.total_timeout += 1
        raise

    finally:
        duration = (datetime.now() - start_time).total_seconds()
        if metrics.total_received % 100 == 0:
            logger.info(f"Ack metrics: {metrics}, last duration: {duration}s")
```

## Testing Acknowledgment

Test acknowledgment behavior:

```python
import pytest
from faststream.gcp import TestGCPBroker

@pytest.mark.asyncio
async def test_auto_ack_success():
    async with TestGCPBroker(broker) as test_broker:
        # Publish test message
        await test_broker.publish({"test": "data"}, "events")

        # Verify handler was called and message was acked
        handle_auto_ack.mock.assert_called_once()
        # In test mode, successful completion means ack

@pytest.mark.asyncio
async def test_manual_ack():
    async with TestGCPBroker(broker) as test_broker:
        # Test manual acknowledgment
        await test_broker.publish({"test": "data"}, "important-events")

        # Verify manual ack was called
        message_mock = handle_manual_ack.mock.call_args[0][1]
        message_mock.ack.assert_called_once()
```

## Common Pitfalls

- **Not handling redeliveries**: Always assume messages can be delivered multiple times
- **Acking too early**: Don't acknowledge before processing is truly complete
- **Ignoring deadlines**: Ensure processing completes within ack deadline
- **Missing error handling**: Unhandled exceptions prevent acknowledgment
- **Blocking operations**: Long synchronous operations can exceed deadlines
