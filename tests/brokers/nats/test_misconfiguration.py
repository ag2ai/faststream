import pytest
from nats.aio.client import Client

from faststream._internal._compat import ExceptionGroup
from faststream.exceptions import SetupError
from faststream.nats import NatsRouter
from faststream.nats.broker.broker import NatsBroker
from faststream.rabbit import RabbitRouter


@pytest.mark.nats()
def test_use_only_nats_router() -> None:
    broker = NatsBroker()
    router = RabbitRouter()

    with pytest.raises(SetupError):
        broker.include_router(router)

    routers = [NatsRouter(), RabbitRouter()]

    with pytest.raises(SetupError):
        broker.include_routers(routers)


@pytest.mark.nats()
def test_driver_class_annotation_names_the_import_to_use() -> None:
    expected = (
        "`handler` parameter `client` is annotated with"
        " `nats.aio.client.Client`,"
        " which FastStream cannot inject.\n"
        "Use the context annotation instead:\n"
        "\n    from faststream.nats.annotations import Client\n"
    )

    broker = NatsBroker()

    with pytest.raises(ExceptionGroup) as excinfo:

        @broker.subscriber("test")
        async def handler(client: Client) -> None: ...

    assert [str(e) for e in excinfo.value.exceptions] == [expected]
