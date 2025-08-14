"""Example showing GCP Pub/Sub publishing with attributes."""

import os
from datetime import datetime
from faststream import FastStream, Logger
from faststream.gcp import GCPBroker, GCPResponse, MessageAttributes, OrderingKey

broker = GCPBroker(
    project_id=os.getenv("GCP_PROJECT_ID", "test-project"),
    emulator_host=os.getenv("PUBSUB_EMULATOR_HOST"),
)
app = FastStream(broker)


@broker.subscriber("orders-sub", topic="orders")
async def handle_order(
    order: dict,
    attrs: MessageAttributes,
    ordering_key: OrderingKey,
    logger: Logger,
) -> None:
    """Handle incoming orders with attributes."""
    user_id = attrs.get("user_id", "unknown")
    priority = attrs.get("priority", "normal")

    logger.info(f"Processing {priority} priority order for user {user_id}")

    # Manual publishing with custom attributes
    await broker.publish(
        {"order_id": order["id"], "status": "processing", "user_id": user_id},
        topic="order-status",
        attributes={
            "user_id": user_id,
            "order_id": str(order["id"]),
            "status": "processing",
            "processed_at": datetime.now().isoformat(),
            "processor": "order_handler",
        },
        ordering_key=f"user-{user_id}",  # Maintain order per user
    )


@broker.subscriber("payments-sub", topic="payments")
@broker.publisher("notifications")
async def process_payment(
    payment: dict,
    attrs: MessageAttributes,
    logger: Logger,
) -> GCPResponse:
    """Process payment and return notification with attributes."""
    user_id = attrs.get("user_id")
    amount = payment["amount"]

    logger.info(f"Processing ${amount} payment for user {user_id}")

    # Return response with rich attributes
    return GCPResponse(
        body=f"Payment of ${amount} processed successfully!",
        attributes={
            "user_id": user_id,
            "payment_id": str(payment["id"]),
            "amount": str(amount),
            "status": "success",
            "processed_at": datetime.now().isoformat(),
            "notification_type": "payment_success",
            "channel": "push",
        },
        ordering_key=f"notifications-{user_id}",
    )


@broker.subscriber("inventory-sub", topic="inventory")
@broker.publisher("restocking")
async def check_inventory(
    item: dict,
    attrs: MessageAttributes,
    logger: Logger,
) -> str | GCPResponse:
    """Check inventory and conditionally return enriched response."""
    item_id = item["id"]
    current_stock = item["stock"]
    min_threshold = int(attrs.get("min_threshold", "10"))

    logger.info(f"Checking inventory for item {item_id}: {current_stock} units")

    if current_stock < min_threshold:
        # Low stock - send enriched restocking message
        return GCPResponse(
            body={
                "item_id": item_id,
                "current_stock": current_stock,
                "suggested_reorder": min_threshold * 3,
                "urgency": "high" if current_stock < min_threshold // 2 else "medium",
            },
            attributes={
                "item_id": str(item_id),
                "current_stock": str(current_stock),
                "min_threshold": str(min_threshold),
                "urgency": "high" if current_stock < min_threshold // 2 else "medium",
                "category": attrs.get("category", "general"),
                "supplier": attrs.get("supplier", "default"),
                "restock_requested_at": datetime.now().isoformat(),
            },
            ordering_key=f"restock-{item_id}",
        )
    else:
        # Stock OK - simple message
        return f"Item {item_id} stock OK: {current_stock} units"


@broker.subscriber("notifications-sub", topic="notifications")
async def send_notification(
    message: str,
    attrs: MessageAttributes,
    logger: Logger,
) -> None:
    """Send notifications based on attributes."""
    notification_type = attrs.get("notification_type", "general")
    user_id = attrs.get("user_id", "unknown")
    channel = attrs.get("channel", "email")

    logger.info(f"Sending {notification_type} notification to user {user_id} via {channel}")
    logger.info(f"Message: {message}")


@broker.subscriber("restocking-sub", topic="restocking")
async def handle_restocking(
    restock_info: dict,
    attrs: MessageAttributes,
    logger: Logger,
) -> None:
    """Handle restocking requests with urgency-based processing."""
    item_id = restock_info["item_id"]
    urgency = attrs.get("urgency", "medium")
    supplier = attrs.get("supplier", "default")

    logger.info(f"Restocking item {item_id} with {urgency} urgency from {supplier}")

    if urgency == "high":
        logger.warning(f"HIGH URGENCY: Item {item_id} needs immediate restocking!")


@broker.subscriber("order-status-sub", topic="order-status")
async def track_order_status(
    status_update: dict,
    attrs: MessageAttributes,
    logger: Logger,
) -> None:
    """Track order status updates."""
    order_id = attrs.get("order_id", "unknown")
    user_id = attrs.get("user_id", "unknown")
    status = status_update["status"]

    logger.info(f"Order {order_id} for user {user_id} is now: {status}")


@app.after_startup
async def publish_example_data():
    """Publish example messages with rich attributes."""

    # Publish an order
    await broker.publish(
        {"id": 12345, "items": ["laptop", "mouse"], "total": 899.99},
        topic="orders",
        attributes={
            "user_id": "user-789",
            "priority": "high",
            "source": "web",
            "campaign": "black-friday",
        },
        ordering_key="user-789",
    )

    # Publish a payment
    await broker.publish(
        {"id": 67890, "amount": 899.99, "method": "credit_card"},
        topic="payments",
        attributes={
            "user_id": "user-789",
            "payment_method": "visa",
            "country": "US",
        },
    )

    # Publish inventory items with different stock levels
    inventory_items = [
        {"id": 1001, "name": "laptop", "stock": 3},  # Low stock
        {"id": 1002, "name": "mouse", "stock": 25},  # OK stock
        {"id": 1003, "name": "keyboard", "stock": 1},  # Very low stock
    ]

    for item in inventory_items:
        await broker.publish(
            item,
            topic="inventory",
            attributes={
                "category": "electronics",
                "supplier": "tech-corp",
                "min_threshold": "10",
                "warehouse": "west-coast",
            },
        )


if __name__ == "__main__":
    app.run()
