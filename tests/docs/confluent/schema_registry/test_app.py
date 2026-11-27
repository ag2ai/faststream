from collections.abc import Generator
from unittest.mock import MagicMock

import pytest

from docs.docs_src.confluent.schema_registry import app as module
from faststream.confluent import KafkaMessage, TestKafkaBroker


class FakeSchema:
    def __init__(self, schema_id: int = 7) -> None:
        self.schema_id = schema_id
        self.schema = self
        self.schema_str = module.AVRO_SCHEMA_JSON


class FakeRegistry:
    def get_latest_version(self, subject: str) -> FakeSchema:
        assert subject == "people-value"
        return FakeSchema()

    def get_schema(self, schema_id: int) -> FakeSchema:
        return FakeSchema(schema_id)


@pytest.fixture()
def fake_registry(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    monkeypatch.setattr(module, "registry", FakeRegistry())
    module.LATEST_SCHEMAS.clear()
    module.SCHEMAS_BY_ID.clear()

    yield

    module.LATEST_SCHEMAS.clear()
    module.SCHEMAS_BY_ID.clear()


def test_encoder_decoder_roundtrip(fake_registry: None) -> None:
    schema_id, schema = module.get_latest_schema("people-value")
    body = module.encode_avro(module.Person(name="Ada", age=36), schema, schema_id)

    assert body[:1] == b"\x00"
    assert int.from_bytes(body[1:5], "big") == schema_id
    assert module.decode_avro(body) == {"name": "Ada", "age": 36}


def test_batch_decoder(fake_registry: None) -> None:
    schema_id, schema = module.get_latest_schema("people-value")
    body = module.encode_avro(module.Person(name="Ada", age=36), schema, schema_id)
    msg = KafkaMessage(
        raw_message=MagicMock(),
        body=[body, body],
        consumer=MagicMock(),
    )

    assert module.schema_registry_decoder(msg) == [
        {"name": "Ada", "age": 36},
        {"name": "Ada", "age": 36},
    ]


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_app_roundtrip(fake_registry: None) -> None:
    async with TestKafkaBroker(module.broker) as br:
        await br.publish(module.Person(name="Ada", age=36), "people")

        module.handle_person.mock.assert_called_once_with(
            {"name": "Ada", "age": 36},
        )


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_batch_publish(fake_registry: None) -> None:
    async with TestKafkaBroker(module.broker) as br:
        await br.publish_batch(
            module.Person(name="Ada", age=36),
            module.Person(name="Grace", age=85),
            topic="people",
        )

        assert module.handle_person.mock.call_count == 2
        assert [call.args[0] for call in module.handle_person.mock.call_args_list] == [
            {"name": "Ada", "age": 36},
            {"name": "Grace", "age": 85},
        ]
