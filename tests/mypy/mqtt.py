from faststream.mqtt import (
    MQTTBroker,
    MQTTPublisher,
    MQTTRoute,
    MQTTRouter,
    QoS,
    TestMQTTBroker,
    Will,
    WillProperties,
)
from faststream.mqtt.fastapi import MQTTRouter as FastAPIMQTTRouter

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


router = MQTTRouter()


@router.subscriber("test")
@router.publisher("test2", skip_none=True)
def handle_router_skip_none() -> None: ...


@router.subscriber("test")
@router.publisher("test2", skip_none=True)
async def handle_router_skip_none_async() -> None: ...


def sync_handler() -> None: ...


async def async_handler() -> None: ...


MQTTRouter(
    handlers=(
        MQTTRoute(sync_handler, "test"),
        MQTTRoute(async_handler, "test"),
        MQTTRoute(
            sync_handler,
            "test",
            publishers=(MQTTPublisher("test2", skip_none=True),),
        ),
    ),
)


fastapi_router = FastAPIMQTTRouter()


@fastapi_router.subscriber("test")
@fastapi_router.publisher("test2", skip_none=True)
def handle_fastapi_skip_none() -> None: ...


@fastapi_router.subscriber("test")
@fastapi_router.publisher("test2", skip_none=True)
async def handle_fastapi_skip_none_async() -> None: ...


async def check_skip_none_publisher() -> None:
    broker = MQTTBroker()

    skip_publisher = broker.publisher("test", skip_none=True)
    await skip_publisher.publish(None)
    await skip_publisher.request(None)
