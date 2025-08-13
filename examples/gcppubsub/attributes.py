from faststream import FastStream, Logger
from faststream.gcp import GCPBroker, GCPMessage

broker = GCPBroker(project_id="test-project")
app = FastStream(broker)


@broker.subscriber("priority-sub", topic="priority-events")
async def handle_priority(msg: GCPMessage, logger: Logger):
    priority = msg.attributes.get("priority", "normal")
    source = msg.attributes.get("source", "unknown")

    logger.info(f"Priority: {priority}, Source: {source}, Body: {msg.body}")


@app.after_startup
async def test_send():
    # Send with attributes
    await broker.publish(
        "High priority message",
        "priority-events",
        attributes={"priority": "high", "source": "api"}
    )

    await broker.publish(
        "Normal message",
        "priority-events",
        attributes={"priority": "normal", "source": "web"}
    )
