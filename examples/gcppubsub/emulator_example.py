#!/usr/bin/env python3
"""
GCP Pub/Sub example using the actual GCP emulator.

This example shows how to test against a real GCP Pub/Sub emulator instance.

Prerequisites:
1. Install Google Cloud SDK: https://cloud.google.com/sdk/docs/install
2. Start the Pub/Sub emulator:
   gcloud beta emulators pubsub start --project=test-project --host-port=0.0.0.0:8085

3. Set environment variable:
   export PUBSUB_EMULATOR_HOST=localhost:8085

4. Run this example:
   python examples/gcppubsub/emulator_example.py
"""

import asyncio
import os
from typing import Dict

import pytest

from faststream import FastStream, Logger
from faststream.gcppubsub import GCPPubSubBroker, TestGCPPubSubBroker


def check_emulator():
    """Check if emulator is available."""
    emulator_host = os.getenv("PUBSUB_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("PUBSUB_EMULATOR_HOST not set. Please start the Pub/Sub emulator.")
    return emulator_host


# Configure broker for emulator
emulator_host = os.getenv("PUBSUB_EMULATOR_HOST", "localhost:8085")
broker = GCPPubSubBroker(
    project_id="test-project",
    emulator_host=emulator_host,
)
app = FastStream(broker)

# Message storage for verification
received_messages = []


@broker.subscriber("user-events-sub", topic="user-events", create_subscription=True)
async def handle_user_event(message: Dict[str, str], logger: Logger):
    """Handle user events."""
    logger.info(f"Received user event: {message}")
    received_messages.append(message)

    return {
        "status": "processed",
        "user_id": message.get("user_id"),
        "processed_at": "2024-01-15T10:30:00Z"
    }


@broker.subscriber("order-events-sub", topic="order-events", create_subscription=True)
async def handle_order_event(message: Dict[str, str], logger: Logger):
    """Handle order events."""
    logger.info(f"Received order event: {message}")
    received_messages.append(message)

    # Simulate processing
    order_id = message.get("order_id", "unknown")
    return {
        "order_id": order_id,
        "status": "confirmed" if order_id != "error" else "failed"
    }


@broker.publisher("notifications")
async def send_notification(data: Dict[str, str]) -> Dict[str, str]:
    """Send notification."""
    return {
        "notification_id": f"notif-{data.get('user_id', 'unknown')}",
        "message": data.get("message", ""),
        "sent": True
    }


async def test_with_emulator():
    """Test with actual GCP Pub/Sub emulator."""
    print("🧪 Testing with GCP Pub/Sub emulator...")

    # Clear previous messages
    received_messages.clear()

    async with broker:
        # Give broker time to set up
        await asyncio.sleep(0.5)

        # Test 1: Publish user event
        print("📤 Publishing user event...")
        await broker.publish(
            {
                "user_id": "user123",
                "action": "login",
                "timestamp": "2024-01-15T10:30:00Z"
            },
            topic="user-events"
        )

        # Test 2: Publish order event
        print("📤 Publishing order event...")
        await broker.publish(
            {
                "order_id": "order456",
                "user_id": "user123",
                "amount": 99.99,
                "items": ["item1", "item2"]
            },
            topic="order-events"
        )

        # Test 3: Send notification using publisher decorator
        print("📤 Sending notification...")
        result = await send_notification({
            "user_id": "user123",
            "message": "Welcome to our service!"
        })
        print(f"Notification result: {result}")

        # Wait for message processing
        print("⏳ Waiting for messages to be processed...")
        await asyncio.sleep(2)

        # Verify results
        print(f"📥 Received {len(received_messages)} messages:")
        for i, msg in enumerate(received_messages, 1):
            print(f"  {i}. {msg}")

        assert len(received_messages) >= 2, f"Expected at least 2 messages, got {len(received_messages)}"
        print("✅ Emulator test passed!")


async def test_with_test_broker():
    """Test with TestGCPPubSubBroker (for comparison)."""
    print("\n🧪 Testing with TestGCPPubSubBroker...")

    async with TestGCPPubSubBroker(broker, with_real=False) as test_broker:
        # Test publishing
        result = await test_broker.publish("Test message", topic="user-events")
        print(f"Test broker result: {result}")

        # Test notification
        notification_result = await send_notification({
            "user_id": "test-user",
            "message": "Test notification"
        })
        print(f"Test notification result: {notification_result}")

        print("✅ Test broker test passed!")


async def main():
    """Main test function."""
    try:
        emulator_host = check_emulator()
        print(f"🚀 Using Pub/Sub emulator at: {emulator_host}")

        # Test with emulator
        await test_with_emulator()

        # Test with test broker for comparison
        await test_with_test_broker()

        print("\n🎉 All tests passed!")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()


@pytest.mark.asyncio
async def test_emulator_integration():
    """Pytest test for emulator integration."""
    emulator_host = check_emulator()
    print(f"Using emulator at: {emulator_host}")

    received_messages.clear()

    async with broker:
        await asyncio.sleep(0.2)  # Brief setup time

        # Publish test message
        await broker.publish(
            {"user_id": "pytest-user", "action": "test"},
            topic="user-events"
        )

        # Wait for processing
        await asyncio.sleep(1)

        # Verify
        assert len(received_messages) >= 1
        assert any("pytest-user" in str(msg) for msg in received_messages)


if __name__ == "__main__":
    asyncio.run(main())
