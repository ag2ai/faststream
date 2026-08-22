from faststream import Config
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
from faststream.mqtt.fastapi import MQTTRouter as FastAPIRouter

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


async def async_handler(msg: str) -> None: ...


# --- Config placeholders ------------------------------------------------------
#
# A placeholder is accepted on address parameters and nowhere else, and that
# boundary is the signature alone — there is no runtime guard (ADR-0002). The
# `type: ignore` comments below are therefore assertions, not suppressions:
# `warn_unused_ignores` is on, so if any of these positions ever starts
# type-checking, the ignore goes unused and the build fails.


def check_config_on_subscriber_address_params(
    broker: MQTTBroker | FastAPIRouter | MQTTRouter,
) -> None:
    broker.subscriber(Config("TOPIC"))
    broker.subscriber("sensors/temp", shared=Config("GROUP"))
    broker.subscriber(Config("TOPIC"), shared=Config("GROUP"))
    broker.subscriber(Config("TOPIC"), shared="group")
    broker.subscriber(Config("TOPIC"), qos=QoS.AT_LEAST_ONCE, max_workers=2)


def check_config_on_publisher_address_params(
    broker: MQTTBroker | FastAPIRouter | MQTTRouter,
) -> None:
    broker.publisher(Config("TOPIC"))
    broker.publisher(Config("TOPIC"), qos=QoS.AT_LEAST_ONCE, retain=True)


def check_config_on_router_containers() -> None:
    MQTTRouter(
        handlers=(
            MQTTRoute(async_handler, Config("TOPIC")),
            MQTTRoute(async_handler, "sensors/temp", shared=Config("GROUP")),
            MQTTRoute(
                async_handler,
                "sensors/temp",
                publishers=(MQTTPublisher(Config("TOPIC")),),
            ),
        ),
    )


async def check_config_is_rejected_by_runtime_publishing() -> None:
    broker = MQTTBroker()

    await broker.publish(None, Config("TOPIC"))  # type: ignore[arg-type]
    await broker.publish(None, "test", reply_to=Config("REPLY"))  # type: ignore[arg-type]
    await broker.request(None, Config("TOPIC"))  # type: ignore[arg-type]

    publisher = broker.publisher("test")
    await publisher.publish(None, Config("TOPIC"))  # type: ignore[arg-type]
    await publisher.request(None, Config("TOPIC"))  # type: ignore[arg-type]


def check_config_is_rejected_on_structural_params(broker: MQTTBroker) -> None:
    broker.subscriber("test", qos=Config("QOS"))  # type: ignore[arg-type]
    broker.subscriber("test", max_workers=Config("WORKERS"))  # type: ignore[arg-type]
    broker.subscriber("test", ack_policy=Config("ACK"))  # type: ignore[arg-type]
    broker.subscriber("test", no_reply=Config("NO_REPLY"))  # type: ignore[arg-type]
    broker.publisher("test", qos=Config("QOS"))  # type: ignore[arg-type]
    broker.publisher("test", retain=Config("RETAIN"))  # type: ignore[arg-type]
