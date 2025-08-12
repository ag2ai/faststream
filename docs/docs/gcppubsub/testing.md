# Testing GCP Pub/Sub Applications

FastStream provides comprehensive testing utilities for GCP Pub/Sub applications, allowing you to test your message handlers and publishers without requiring a real GCP Pub/Sub connection.

## Key Testing Concepts

The GCP Pub/Sub testing framework follows FastStream's established patterns:

- **TestGCPPubSubBroker**: Main test broker class that patches the real broker
- **FakeGCPPubSubProducer**: Fake producer that routes messages to matching subscribers  
- **Message Building**: Utilities to create test messages with proper structure
- **Handler Execution**: Fake producer automatically calls `process_message()` on matching handlers
- **Topic Matching**: Simple topic-based routing to find appropriate subscribers

## Test Broker

The `TestGCPPubSubBroker` class provides a fake implementation that simulates GCP Pub/Sub behavior following FastStream's established patterns:

```python
import pytest
from faststream.gcppubsub import GCPPubSubBroker
from faststream.gcppubsub.testing import TestGCPPubSubBroker, create_test_message

broker = GCPPubSubBroker(project_id="test-project")

@broker.subscriber("test-subscription", topic="test-topic")
async def handle_message(message: str):
    return f"Processed: {message}"

@pytest.mark.asyncio
async def test_message_handling():
    async with TestGCPPubSubBroker(broker) as test_broker:
        # The fake producer automatically routes messages to subscribers
        await test_broker.broker.config.producer.publish(
            topic="test-topic",
            data=b"Hello World"
        )
```

## Testing Message Handlers

### Basic Message Testing

```python
from faststream.gcppubsub import GCPPubSubBroker
from faststream.gcppubsub.testing import TestGCPPubSubBroker, create_test_message

broker = GCPPubSubBroker(project_id="test-project")

@broker.subscriber("user-events", topic="users")
async def handle_user_event(message: dict):
    user_id = message["user_id"]
    action = message["action"]
    return {"status": "processed", "user_id": user_id}

async def test_user_event_handler():
    async with TestGCPPubSubBroker(broker) as test_broker:
        # Create test message
        test_data = b'{"user_id": "123", "action": "login"}'
        
        # Publish using fake producer - handler is called automatically
        await test_broker.broker.config.producer.publish(
            topic="users",
            data=test_data,
            attributes={"content-type": "application/json"}
        )
```

### Testing with Message Attributes

```python
async def test_message_with_attributes():
    async with TestGCPPubSubBroker(broker) as test_broker:
        # Create test message with attributes
        test_msg = create_test_message(
            data="test data",
            attributes={
                "source": "test",
                "priority": "high"
            },
            ordering_key="user-123"
        )
        
        # Publish using fake producer
        await test_broker.broker.config.producer.publish(
            topic="test-topic",
            data=test_msg.data,
            attributes=test_msg.attributes,
            ordering_key=test_msg.ordering_key
        )
```

## Testing Publishers

### Publisher Decorator Testing

```python
@broker.publisher("notifications")
async def send_notification(data: dict) -> dict:
    return {
        "notification_id": f"notif-{data['user_id']}",
        "message": data["message"]
    }

async def test_publisher():
    async with TestGCPPubSubBroker(broker) as test_broker:
        result = await send_notification({
            "user_id": "123",
            "message": "Welcome!"
        })
        
        assert result["notification_id"] == "notif-123"
```

### Direct Publishing

```python
async def test_direct_publishing():
    async with TestGCPPubSubBroker(broker) as test_broker:
        # Test direct publishing using fake producer
        message_id = await test_broker.broker.config.producer.publish(
            topic="test-topic",
            data=b"Hello World"
        )
        
        # Verify message was published
        assert message_id is not None
        assert message_id.startswith("test-msg-")
```

## Message Building Utilities

### Creating Test Messages

```python
from faststream.gcppubsub.testing import create_test_message

async def test_message_creation():
    # Create a simple test message
    msg = create_test_message(
        data="Hello World",
        attributes={"source": "test"},
        message_id="custom-id-123"
    )
    
    assert msg.data == b"Hello World"
    assert msg.attributes["source"] == "test"
    assert msg.message_id == "custom-id-123"

async def test_with_ordering_key():
    # Create message with ordering key
    msg = create_test_message(
        data="Ordered message",
        ordering_key="user-456"
    )
    
    assert msg.ordering_key == "user-456"
```

### Batch Message Testing

```python
async def test_batch_publishing():
    async with TestGCPPubSubBroker(broker) as test_broker:
        # Create multiple test messages
        messages = [
            create_test_message(data=f"Message {i}")
            for i in range(3)
        ]
        
        # Test batch publishing
        message_ids = await test_broker.broker.config.producer.publish_batch(
            topic="test-topic",
            messages=messages
        )
        
        assert len(message_ids) == 3
        assert all(msg_id.startswith("test-msg-") for msg_id in message_ids)
```

## Testing with Real GCP Pub/Sub

For integration tests, you can use the `with_real=True` parameter:

```python
async def test_with_real_pubsub():
    # Requires GCP Pub/Sub emulator or real GCP credentials
    async with TestGCPPubSubBroker(broker, with_real=True) as test_broker:
        # This will use actual GCP Pub/Sub
        await test_broker.publish("Real message", "real-topic")
```

## Pytest Integration

### Using pytest fixtures

```python
import pytest
from faststream.gcppubsub import GCPPubSubBroker
from faststream.gcppubsub.testing import TestGCPPubSubBroker

@pytest.fixture
async def test_broker():
    broker = GCPPubSubBroker(project_id="test-project")
    
    @broker.subscriber("test-sub", topic="test-topic")
    async def test_handler(message: str):
        return f"Processed: {message}"
    
    async with TestGCPPubSubBroker(broker) as tb:
        yield tb

@pytest.mark.asyncio
async def test_with_fixture(test_broker):
    await test_broker.publish("Test message", "test-topic")
    messages = test_broker.get_published_messages()
    assert len(messages) == 1
```

### Test configuration

```python
# conftest.py
import pytest

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    import asyncio
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
```

## Advanced Testing

### Request-Response Pattern Testing

```python
async def test_request_response():
    async with TestGCPPubSubBroker(broker) as test_broker:
        # Test request-response (if implemented via correlation IDs)
        try:
            response = await test_broker.broker.config.producer.request(
                topic="test-topic",
                data=b"Request message",
                attributes={"reply_needed": "true"}
            )
            print(f"Received response: {response}")
            
        except Exception as e:
            # GCP Pub/Sub doesn't natively support request-reply
            print(f"Request failed as expected: {e}")
```

### Testing Error Handling

```python
@broker.subscriber("error-topic", topic="errors")
async def error_handler(message: str):
    if message == "error":
        raise ValueError("Test error")
    return "success"

async def test_error_handling():
    async with TestGCPPubSubBroker(broker) as test_broker:
        # Test successful processing
        await test_broker.broker.config.producer.publish(
            topic="errors", 
            data=b"success"
        )
        
        # Test error handling
        with pytest.raises(ValueError):
            await test_broker.broker.config.producer.publish(
                topic="errors", 
                data=b"error"
            )
```

## Complete Testing Example

Here's a comprehensive example showing the full testing workflow:

```python
import pytest
from faststream import FastStream
from faststream.gcppubsub import GCPPubSubBroker
from faststream.gcppubsub.testing import TestGCPPubSubBroker, create_test_message

# Set up broker and app
broker = GCPPubSubBroker(project_id="test-project")
app = FastStream(broker)

@broker.subscriber("user-events", topic="users")
async def process_user_event(message: dict):
    return {"processed": True, "user_id": message["user_id"]}

@broker.publisher("notifications")
async def send_notification(data: dict) -> dict:
    return {"sent": True, "data": data}

class TestCompleteWorkflow:
    @pytest.mark.asyncio
    async def test_end_to_end(self):
        """Complete end-to-end test."""
        async with TestGCPPubSubBroker(broker) as test_broker:
            # 1. Create test message
            msg = create_test_message(
                data='{"user_id": "123", "action": "signup"}',
                attributes={"source": "web"}
            )
            
            # 2. Publish message - handler automatically called
            message_id = await test_broker.broker.config.producer.publish(
                topic="users",
                data=msg.data,
                attributes=msg.attributes
            )
            
            # 3. Test publisher decorator
            result = await send_notification({"message": "Welcome!"})
            assert result["sent"] is True
            
            # 4. Verify message was processed
            assert message_id.startswith("test-msg-")
```

## Best Practices

1. **Use `create_test_message()`** for creating properly structured test messages
2. **Access fake producer** via `test_broker.broker.config.producer`
3. **Test both success and error cases** by publishing different message content
4. **Use descriptive assertions** to verify expected behavior
5. **Mock external dependencies** in message handlers
6. **Test publisher decorators** by calling them directly
7. **Handle async operations** properly with `pytest.mark.asyncio`

## Running Tests

```bash
# Install test dependencies
pip install pytest pytest-asyncio

# Run tests
pytest tests/

# Run with coverage
pytest --cov=your_app tests/

# Run specific test file
pytest tests/test_gcppubsub.py -v
```