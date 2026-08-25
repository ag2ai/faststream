from faststream.mqtt import MQTTBroker, QoS, TestMQTTBroker, Will, WillProperties

MQTTBroker("mqtts://localhost")
MQTTBroker(url="mqtt://localhost:1884")
MQTTBroker(host="localhost", port=1884)
MQTTBroker(
    version="5.0",
    will=Will(
        topic="status/service",
        payload=b"offline",
        qos=QoS.AT_LEAST_ONCE,
        retain=True,
        properties=WillProperties(content_type="text/plain"),
    ),
)


async def check_multiple_test_brokers() -> None:
    async with TestMQTTBroker(MQTTBroker()) as br1:
        await br1.publish(None, "test")

    async with TestMQTTBroker(
        MQTTBroker(),
        MQTTBroker(),
    ) as (br1, br2):
        await br1.publish(None, "test")
        await br2.publish(None, "test")
