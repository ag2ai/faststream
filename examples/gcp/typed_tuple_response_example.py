"""Example showing GCP Pub/Sub typed tuple response API."""

import os
from datetime import datetime
from faststream import FastStream, Logger
from faststream.gcp import (
    GCPBroker,
    MessageAttributes,
    ResponseAttributes,
    ResponseOrderingKey,
)

broker = GCPBroker(
    project_id=os.getenv("GCP_PROJECT_ID", "test-project"),
    emulator_host=os.getenv("PUBSUB_EMULATOR_HOST"),
)
app = FastStream(broker)


@broker.subscriber("events-sub", topic="events")
@broker.publisher("processed-events")
async def process_event_explicit(
    event: dict,
    attrs: MessageAttributes,
    logger: Logger,
) -> tuple:
    """Process event using explicit type markers."""

    event_id = event.get("id", "unknown")
    user_id = attrs.get("user_id", "anonymous")

    logger.info(f"Processing event {event_id} for user {user_id}")

    # Return tuple with explicit type markers - order doesn't matter!
    return (
        {"event_id": event_id, "processed": True},  # Message body
        ResponseAttributes({  # Explicit attributes marker
            "processor": "event_handler",
            "timestamp": datetime.now().isoformat(),
            "user_id": user_id,
        }),
        ResponseOrderingKey(f"user-{user_id}"),  # Explicit ordering key
    )


@broker.subscriber("orders-sub", topic="orders")
@broker.publisher("order-updates")
async def process_order_flexible(
    order: dict,
    attrs: MessageAttributes,
    logger: Logger,
) -> tuple:
    """Process order with flexible tuple ordering."""

    order_id = order["id"]
    priority = attrs.get("priority", "normal")

    # You can return items in ANY order when using explicit types!
    # The types themselves identify what each item is
    return (
        ResponseOrderingKey(f"order-{order_id}"),  # Ordering key first
        ResponseAttributes({  # Then attributes
            "order_id": str(order_id),
            "status": "processing",
            "priority": priority,
        }),
        {"order_id": order_id, "status": "processing"},  # Message body last
    )


@broker.subscriber("metrics-sub", topic="metrics")
@broker.publisher("alerts")
async def check_metrics_conditional(
    metric: dict,
    attrs: MessageAttributes,
    logger: Logger,
) -> tuple:
    """Conditionally include attributes based on metric value."""

    metric_name = metric["name"]
    value = metric["value"]
    threshold = float(attrs.get("threshold", "100"))

    if value > threshold:
        # Alert with full attributes
        return (
            f"ALERT: {metric_name} exceeded threshold",
            ResponseAttributes({
                "severity": "high" if value > threshold * 1.5 else "medium",
                "metric": metric_name,
                "value": str(value),
                "threshold": str(threshold),
            }),
            ResponseOrderingKey(f"alert-{metric_name}"),
        )
    else:
        # OK status with minimal attributes
        return (
            f"OK: {metric_name} within limits",
            ResponseAttributes({"status": "ok", "metric": metric_name}),
        )


@broker.subscriber("notifications-sub", topic="notifications")
@broker.publisher("email-queue")
async def prepare_notification(
    notification: str,
    attrs: MessageAttributes,
    logger: Logger,
) -> tuple:
    """Prepare notification for email queue."""

    user_id = attrs.get("user_id", "unknown")

    # All explicit with type markers
    return (
        notification,
        ResponseAttributes({"channel": "email", "user_id": user_id}),
        ResponseOrderingKey(f"email-{user_id}"),
    )


@broker.subscriber("logs-sub", topic="logs")
@broker.publisher("log-storage")
async def process_logs(
    log_entry: dict,
    attrs: MessageAttributes,
    logger: Logger,
) -> tuple:
    """Process log entries with just attributes, no ordering key."""

    level = log_entry.get("level", "info")

    # Just message and attributes - no ordering key needed
    return (
        log_entry,
        ResponseAttributes({
            "level": level,
            "processed_at": datetime.now().isoformat(),
            "source": attrs.get("source", "unknown"),
        }),
    )


@broker.subscriber("processed-events-sub", topic="processed-events")
async def log_processed(msg: dict, attrs: MessageAttributes, logger: Logger) -> None:
    """Log processed events."""
    logger.info(f"Processed event {msg.get('event_id')}", extra=dict(attrs))


@broker.subscriber("order-updates-sub", topic="order-updates")
async def log_order_update(msg: dict, attrs: MessageAttributes, logger: Logger) -> None:
    """Log order updates."""
    logger.info(f"Order {attrs.get('order_id')} status: {attrs.get('status')}")


@broker.subscriber("alerts-sub", topic="alerts")
async def handle_alert(msg: str, attrs: MessageAttributes, logger: Logger) -> None:
    """Handle alerts."""
    severity = attrs.get("severity", "unknown")
    if severity == "high":
        logger.error(f"HIGH SEVERITY: {msg}")
    else:
        logger.warning(msg)


@app.after_startup
async def publish_example_data():
    """Publish example messages."""

    # Event
    await broker.publish(
        {"id": "evt-001", "type": "login", "timestamp": datetime.now().isoformat()},
        topic="events",
        attributes={"user_id": "user-123", "source": "web"},
    )

    # Order
    await broker.publish(
        {"id": 5001, "items": ["laptop"], "total": 1299.99},
        topic="orders",
        attributes={"user_id": "user-456", "priority": "high"},
    )

    # Metrics - one alert, one OK
    await broker.publish(
        {"name": "cpu_usage", "value": 95},
        topic="metrics",
        attributes={"threshold": "80", "server": "prod-01"},
    )

    await broker.publish(
        {"name": "memory_usage", "value": 65},
        topic="metrics",
        attributes={"threshold": "80", "server": "prod-01"},
    )

    # Notification
    await broker.publish(
        "Your order has been shipped!",
        topic="notifications",
        attributes={"user_id": "user-789", "type": "order_shipped"},
    )

    # Log entry
    await broker.publish(
        {"level": "error", "message": "Connection timeout", "service": "api"},
        topic="logs",
        attributes={"source": "api-server", "environment": "production"},
    )


if __name__ == "__main__":
    app.run()
