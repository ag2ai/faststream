from types import SimpleNamespace
from typing import Annotated, Any, cast

from faststream._internal.configs import BrokerConfig, SubscriberSpecificationConfig
from faststream._internal.constants import EMPTY, PATH_CONTEXT_PREFIX
from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream._internal.endpoint.subscriber.specification import (
    SubscriberSpecification,
    _merge_parameter_schema,
    _path_parameter_schema,
    _schema_enum,
    _schema_types,
)
from faststream.kafka.configs import KafkaBrokerConfig
from faststream.kafka.subscriber.config import KafkaSubscriberSpecificationConfig
from faststream.kafka.subscriber.specification import KafkaSubscriberSpecification
from faststream.specification.schema import SubscriberSpec


class ConcreteSubscriberSpecification(SubscriberSpecification):
    @property
    def channel_labels(self) -> list[str]:
        return []

    def get_schema(self) -> dict[str, SubscriberSpec]:
        return {}


def _handler(declared_call: object, dependant: object = None) -> SimpleNamespace:
    return SimpleNamespace(
        handler=SimpleNamespace(_declared_call=declared_call),
        dependant=dependant,
    )


def test_path_parameter_schema_handles_missing_and_annotated_annotations() -> None:
    def unannotated(value): ...  # type: ignore[no-untyped-def]

    def annotated(value: Annotated[int, "metadata"]) -> None: ...

    assert _path_parameter_schema(_handler(unannotated), "missing") == {}
    assert _path_parameter_schema(_handler(annotated), "value")["type"] == "integer"


def test_schema_enum_and_types_walk_union_branches() -> None:
    schema = {
        "anyOf": [
            {"type": "null"},
            {"const": "fixed"},
            {"enum": ["open", "closed"]},
            "not a schema",
        ],
        "oneOf": [{"type": "integer", "const": 1}],
    }

    assert _schema_enum(schema) == [None, "fixed", "open", "closed", 1]
    assert _schema_types(schema) == ["null", "integer"]


def test_merge_parameter_schema_handles_mixed_types_and_non_enum_fields() -> None:
    target = {"const": "old", "anyOf": [{"type": "null"}], "oneOf": []}
    incoming = {
        "anyOf": [
            {"type": "integer", "enum": [1]},
            {"type": "string", "enum": ["one"]},
        ],
        "title": "Mixed value",
    }

    _merge_parameter_schema(target, incoming)

    assert target == {
        "enum": [1, "one"],
        "type": ["integer", "string"],
        "title": "Mixed value",
    }

    target = {"description": "existing"}
    _merge_parameter_schema(target, {"description": "incoming", "type": "string"})
    assert target == {"description": "existing", "type": "string"}


def test_get_path_parameters_skips_unavailable_and_unmatched_handlers() -> None:
    not_a_path = SimpleNamespace(prefix="other", name="level", default=EMPTY)
    unmatched_path = SimpleNamespace(
        prefix=PATH_CONTEXT_PREFIX,
        name="other",
        default=EMPTY,
    )
    calls = [
        _handler(lambda: None, dependant=None),
        _handler(
            lambda: None,
            dependant=SimpleNamespace(
                custom_fields={"not_path": not_a_path, "unmatched": unmatched_path},
            ),
        ),
    ]
    specification = ConcreteSubscriberSpecification(
        BrokerConfig(),
        SubscriberSpecificationConfig(title_=None, description_=None),
        cast("CallsCollection[Any]", calls),
    )

    assert specification.get_path_parameters(None) == {}
    assert specification.get_path_parameters("logs.{level}") == {"level": {}}


def test_kafka_path_parameters_ignore_non_pattern_channels() -> None:
    specification = KafkaSubscriberSpecification(
        KafkaBrokerConfig(),
        KafkaSubscriberSpecificationConfig(
            title_=None,
            description_=None,
            pattern="logs.{level}",
        ),
        CallsCollection(),
    )

    assert specification.get_path_parameters("logs.static") == {}
