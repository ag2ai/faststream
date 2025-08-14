"""Example showing advanced GCP Pub/Sub attribute usage."""

import os
from datetime import datetime
from faststream import FastStream, Logger
from faststream.gcp import (
    GCPBroker,
    MessageAttributes,
    OrderingKey,
    MessageId,
    UserContext,
    TraceContext,
    PriorityLevel,
    RequiredUserId,
)

broker = GCPBroker(
    project_id=os.getenv("GCP_PROJECT_ID", "test-project"),
    emulator_host=os.getenv("PUBSUB_EMULATOR_HOST"),
)
app = FastStream(broker)


@broker.subscriber("user-events-sub", topic="user-events")
async def handle_user_event(
    msg: dict,
    user_ctx: UserContext,
    trace_ctx: TraceContext,
    priority: PriorityLevel,
    ordering_key: OrderingKey,
    logger: Logger,
) -> None:
    """Handle user events with rich attribute context."""
    logger.info(
        f"Processing {priority} priority event for user {user_ctx['user_id']}",
        extra={
            "trace_id": trace_ctx["trace_id"],
            "user_id": user_ctx["user_id"],
            "tenant_id": user_ctx["tenant_id"],
        }
    )

    # Process based on priority
    if priority == "high":
        await process_immediately(msg, user_ctx)
    else:
        await queue_for_later(msg, user_ctx)


@broker.subscriber("notifications-sub", topic="notifications")
async def handle_notification(
    msg: str,
    user_id: RequiredUserId,  # Will raise if user_id not present
    attrs: MessageAttributes,
    msg_id: MessageId,
    logger: Logger,
) -> None:
    """Handle notifications with required user ID."""
    notification_type = attrs.get("type", "general")

    logger.info(f"Sending {notification_type} notification to user {user_id}")

    await send_notification(user_id, msg, notification_type)


@broker.subscriber("audit-sub", topic="audit-events")
async def handle_audit_event(
    event: dict,
    attrs: MessageAttributes,
    ordering_key: OrderingKey,
    logger: Logger,
) -> None:
    """Handle audit events with full attribute access."""
    # Extract audit context
    action = attrs.get("action", "unknown")
    resource = attrs.get("resource", "unknown")
    actor = attrs.get("actor", "system")

    # Log audit event
    logger.info(
        f"Audit: {actor} performed {action} on {resource}",
        extra={
            "action": action,
            "resource": resource,
            "actor": actor,
            "ordering_key": ordering_key,
            "timestamp": datetime.now().isoformat(),
        }
    )

    # Store in audit log
    await store_audit_event({
        **event,
        "metadata": {
            "action": action,
            "resource": resource,
            "actor": actor,
            "ordering_key": ordering_key,
        }
    })


async def process_immediately(msg: dict, user_ctx: dict) -> None:
    """Process high-priority messages immediately."""
    print(f"🚨 Processing immediately for user {user_ctx['user_id']}: {msg}")


async def queue_for_later(msg: dict, user_ctx: dict) -> None:
    """Queue normal priority messages for later processing."""
    print(f"📝 Queued for user {user_ctx['user_id']}: {msg}")


async def send_notification(user_id: str, message: str, notification_type: str) -> None:
    """Send notification to user."""
    print(f"📧 Sending {notification_type} to {user_id}: {message}")


async def store_audit_event(event: dict) -> None:
    """Store audit event in persistent storage."""
    print(f"📊 Storing audit event: {event}")


@app.after_startup
async def publish_examples():
    """Publish example messages with rich attributes."""

    # High priority user event
    await broker.publish(
        {"action": "login", "timestamp": datetime.now().isoformat()},
        topic="user-events",
        attributes={
            "user_id": "user-123",
            "tenant_id": "tenant-456",
            "session_id": "sess-789",
            "priority": "high",
            "trace_id": "trace-abc",
            "span_id": "span-def",
        },
        ordering_key="user-123",
    )

    # Notification with required user ID
    await broker.publish(
        "Your order has been shipped!",
        topic="notifications",
        attributes={
            "user_id": "user-123",
            "type": "shipping",
            "channel": "email",
        }
    )

    # Audit event
    await broker.publish(
        {"details": "User updated profile"},
        topic="audit-events",
        attributes={
            "action": "update",
            "resource": "user_profile",
            "actor": "user-123",
        },
        ordering_key="audit-user-123",
    )


if __name__ == "__main__":
    app.run()
