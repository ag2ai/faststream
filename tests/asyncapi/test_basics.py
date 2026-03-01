from faststream import FastStream
from faststream.kafka import KafkaBroker
from faststream.specification import AsyncAPI


def test_asyncapi_includes_broker_lazy() -> None:
    broker = KafkaBroker()

    spec = AsyncAPI(schema_version="2.6.0")

    FastStream(broker, specification=spec)

    assert len(spec.brokers) == 1


def test_asyncapi_does_not_duplicate_broker() -> None:
    broker = KafkaBroker()

    spec = AsyncAPI(broker, schema_version="2.6.0")

    FastStream(broker, specification=spec)

    assert len(spec.brokers) == 1
