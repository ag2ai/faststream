import pytest
from zmqtt import MQTTClient

from faststream._internal._compat import ExceptionGroup
from faststream.mqtt import MQTTBroker


@pytest.mark.mqtt()
def test_driver_class_annotation_names_the_import_to_use() -> None:
    expected = (
        "`handler` parameter `client` is annotated with"
        " `zmqtt.client.MQTTClient`,"
        " which FastStream cannot inject.\n"
        "Use the context annotation instead:\n"
        "\n    from faststream.mqtt.annotations import Client\n"
    )

    broker = MQTTBroker()

    with pytest.raises(ExceptionGroup) as excinfo:

        @broker.subscriber("test")
        async def handler(client: MQTTClient) -> None: ...

    assert [str(e) for e in excinfo.value.exceptions] == [expected]
