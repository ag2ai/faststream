from faststream.mqtt import MQTTBroker, TestMQTTBroker

MQTTBroker("mqtts://localhost")
MQTTBroker(url="mqtt://localhost:1884")
MQTTBroker(host="localhost", port=1884)


async def check_multiple_test_brokers() -> None:
    async with TestMQTTBroker(MQTTBroker()) as br1:
        await br1.publish(None, "test")

    async with TestMQTTBroker(
        MQTTBroker(),
        MQTTBroker(),
    ) as (br1, br2):
        await br1.publish(None, "test")
        await br2.publish(None, "test")
