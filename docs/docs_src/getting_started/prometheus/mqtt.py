from faststream import FastStream
from faststream.mqtt import MQTTBroker
from faststream.mqtt.prometheus import MQTTPrometheusMiddleware
from prometheus_client import CollectorRegistry

registry = CollectorRegistry()

broker = MQTTBroker(
    middlewares=(
        MQTTPrometheusMiddleware(registry=registry),
    ),
)
app = FastStream(broker)
