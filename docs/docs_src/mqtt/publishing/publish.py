from faststream import FastStream
from faststream.mqtt import MQTTBroker, MQTTMessage, QoS

broker = MQTTBroker("localhost", version="5.0")
app = FastStream(broker)


@broker.subscriber("devices/alerts")
async def handle_alert(payload: dict, msg: MQTTMessage) -> None:
    print(payload, msg.headers)


@app.after_startup
async def send_alert() -> None:
    await broker.publish(
        {"level": "warning"},
        "devices/alerts",
        qos=QoS.AT_LEAST_ONCE,
        retain=True,
        headers={"source": "docs"},
        correlation_id="alert-1",
    )
