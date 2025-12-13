---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Scheduling messages

JetStream supports scheduling messages to be delivered at a specific time in the future. This is useful for delayed task execution, reminder systems, or any scenario where you need to defer message processing.

## Enabling Message Scheduling

To use message scheduling, you need to create a JetStream with the `allow_msg_schedules=True` parameter:

```python
from faststream import FastStream
from faststream.nats import NatsBroker, JStream, NatsMessage, Schedule

broker = NatsBroker()

@broker.subscriber(
  "test_stream.*",
  stream=JStream("test_stream", allow_msg_schedules=True)
)
async def handle_scheduled_message(msg: NatsMessage) -> None:
  # Process the scheduled message when it arrives
  print(f"Received scheduled message: {msg}")
```

## Publishing Scheduled Messages

To schedule a message for future delivery, use the `Schedule` object when publishing:

```python
from datetime import UTC, datetime, timedelta
from uuid import uuid4

async def publish_scheduled_message() -> None:
  # Connect to the broker
  await broker.connect()

  # Calculate the delivery time (e.g., 3 seconds from now)
  current_time = datetime.now(tz=UTC)
  schedule_time = current_time + timedelta(seconds=3)

  # Define the target subject for the scheduled message
  schedule_target = f"test_stream.{uuid4()}"

  # Publish the message with a schedule
  await broker.publish(
    message={"type": "do_something"},
    subject="test_stream.subject",
    schedule=Schedule(schedule_time, schedule_target),
    stream="test_stream",
    timeout=10,
  )
```

## Complete Example

Here's a full working example that demonstrates scheduled message publishing:

```python
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from faststream import FastStream
from faststream.nats import JStream, NatsBroker, NatsMessage, Schedule

broker = NatsBroker()


@broker.subscriber(
  "test_stream.*",
  stream=JStream("test_stream", allow_msg_schedules=True)
)
async def handle_scheduled_message(msg: NatsMessage) -> None:
  print(f"Message received at {datetime.now(tz=UTC)}")
  print(msg)


async def on_startup() -> None:
  current_time = datetime.now(tz=UTC)
  schedule_time = current_time + timedelta(seconds=3)
  await broker.connect()

  schedule_target = f"test_stream.{uuid4()}"
  await broker.publish(
    message={"type": "do_something"},
    subject="test_stream.subject",
    schedule=Schedule(schedule_time, schedule_target),
    stream="test_stream",
    timeout=10,
  )
  print(f"Message scheduled for delivery at {schedule_time}")


app = FastStream(broker)
app.on_startup(on_startup)

if __name__ == "__main__":
  import asyncio
  asyncio.run(app.run())
```

## Key Points

- **Stream Configuration**: The JetStream must be created with `allow_msg_schedules=True` to enable scheduling
- **Schedule Object**: Takes two parameters:
    - `schedule_time`: A `datetime` object (preferably with UTC timezone) indicating when the message should be delivered
    - `schedule_target`: The subject where the scheduled message will be published, should be unique for every message.
- **Subject Pattern**: The subscriber should use a wildcard pattern (e.g., `"test_stream.*"`) to match the scheduled target subjects
- **Timezone**: Always use timezone-aware datetime objects, preferably UTC, to avoid scheduling issues
