import pytest
from aio_pika import RobustConnection

from faststream._internal._compat import ExceptionGroup
from faststream.exceptions import SetupError
from faststream.nats import NatsRouter
from faststream.rabbit import RabbitBroker, RabbitRouter


@pytest.mark.rabbit()
def test_use_only_rabbit_router() -> None:
    broker = RabbitBroker()
    router = NatsRouter()

    with pytest.raises(SetupError):
        broker.include_router(router)

    routers = [RabbitRouter(), NatsRouter()]

    with pytest.raises(SetupError):
        broker.include_routers(routers)


@pytest.mark.rabbit()
def test_driver_class_annotation_names_the_import_to_use() -> None:
    expected = (
        "`connection` is annotated with"
        " `aio_pika.robust_connection.RobustConnection`,"
        " which FastStream cannot inject.\n"
        "Use the context annotation instead:\n"
        "\n    from faststream.rabbit.annotations import Connection\n"
    )

    broker = RabbitBroker()

    with pytest.raises(ExceptionGroup) as excinfo:

        @broker.subscriber("test")
        async def handler(connection: RobustConnection) -> None: ...

    assert [str(e) for e in excinfo.value.exceptions] == [expected]
