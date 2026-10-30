from abc import abstractmethod
from inspect import Parameter
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

from pydantic import create_model
from typing_extensions import (
    TypeVar as TypeVar313,
)

from faststream._internal._compat import model_schema
from faststream._internal.configs import BrokerConfig, SubscriberSpecificationConfig
from faststream._internal.constants import EMPTY, PATH_CONTEXT_PREFIX
from faststream._internal.endpoint.specification import EndpointSpecification
from faststream._internal.utils.path import PARAM_REGEX
from faststream.exceptions import SetupError
from faststream.specification.asyncapi.message import parse_handler_params
from faststream.specification.asyncapi.utils import to_camelcase

if TYPE_CHECKING:
    from faststream._internal.endpoint.subscriber.call_item import (
        CallsCollection,
    )
    from faststream.specification.schema import SubscriberSpec


def _path_parameter_schema(handler: Any, field_name: str) -> dict[str, Any]:
    declared_call = handler.handler._declared_call
    annotation = get_type_hints(declared_call, include_extras=True).get(
        field_name,
        Parameter.empty,
    )
    if annotation is Parameter.empty:
        return {}

    if get_origin(annotation) is Annotated:
        annotation = get_args(annotation)[0]

    field_definitions: dict[str, Any] = {field_name: (annotation, ...)}
    model = create_model("PathParameter", **field_definitions)
    schema = model_schema(model).get("properties", {}).get(field_name, {})
    return cast("dict[str, Any]", schema)


def _schema_enum(schema: dict[str, Any]) -> list[Any]:
    values = list(schema.get("enum", ()))
    if "const" in schema:
        values.append(schema["const"])
    if schema.get("type") == "null" and "const" not in schema:
        values.append(None)

    for branch in (*schema.get("anyOf", ()), *schema.get("oneOf", ())):
        if isinstance(branch, dict):
            values.extend(_schema_enum(branch))
    return list(dict.fromkeys(values))


def _schema_types(schema: dict[str, Any]) -> list[str]:
    values = [schema["type"]] if isinstance(schema.get("type"), str) else []
    for branch in (*schema.get("anyOf", ()), *schema.get("oneOf", ())):
        if isinstance(branch, dict):
            values.extend(_schema_types(branch))
    return list(dict.fromkeys(values))


def _merge_parameter_schema(
    target: dict[str, Any],
    incoming: dict[str, Any],
) -> None:
    enums = _schema_enum(incoming)
    if enums:
        target.pop("const", None)
        target.pop("anyOf", None)
        target.pop("oneOf", None)
        target["enum"] = list(dict.fromkeys([*target.get("enum", ()), *enums]))
        existing_type = target.get("type", ())
        existing_types = (
            [existing_type] if isinstance(existing_type, str) else existing_type
        )
        schema_types = list(dict.fromkeys([*existing_types, *_schema_types(incoming)]))
        if len(schema_types) == 1:
            target["type"] = schema_types[0]
        elif len(schema_types) > 1:
            target["type"] = schema_types
        if "title" in incoming:
            target.setdefault("title", incoming["title"])
        return

    for key, value in incoming.items():
        target.setdefault(key, value)


T_SpecificationConfig = TypeVar313(
    "T_SpecificationConfig",
    bound=SubscriberSpecificationConfig,
    default=SubscriberSpecificationConfig,
)
T_BrokerConfig = TypeVar313("T_BrokerConfig", bound=BrokerConfig, default=BrokerConfig)


class SubscriberSpecification(
    EndpointSpecification[T_BrokerConfig, T_SpecificationConfig],
):
    __slots__ = ("calls",)

    def __init__(
        self,
        _outer_config: "T_BrokerConfig",
        specification_config: "T_SpecificationConfig",
        calls: "CallsCollection[Any]",
    ) -> None:
        super().__init__(_outer_config, specification_config)
        self.calls = calls

    @property
    def description(self) -> str | None:
        return self.config.description_ or self.calls.description

    @property
    def call_name(self) -> str:
        return self.calls.name or "Subscriber"

    def _channel_key(self, address: str, *, split: bool) -> str:
        """The key one address's channel goes under.

        A `title` names one channel, so `split` prefixes it with the address to
        keep several of them unique.
        """
        if not self.config.title_:
            return f"{address}:{self.call_name}"

        return f"{self.config.title_}:{address}" if split else self.config.title_

    def get_path_parameters(
        self,
        address: str | None,
    ) -> dict[str, dict[str, Any]]:
        """Collect AsyncAPI path-parameter metadata from the subscriber handlers."""
        if address is None:
            return {}

        parameter_names = dict.fromkeys(PARAM_REGEX.findall(address))
        parameters: dict[str, dict[str, Any]] = {name: {} for name in parameter_names}

        for handler in self.calls:
            call = handler.dependant
            if call is None:
                continue

            for field_name, custom in getattr(call, "custom_fields", {}).items():
                if getattr(custom, "prefix", None) != PATH_CONTEXT_PREFIX:
                    continue

                name = custom.name or field_name
                if name not in parameters:
                    continue

                parameter = parameters[name]
                field_schema = _path_parameter_schema(handler, field_name)
                if field_schema:
                    schema = parameter.setdefault("schema", {})
                    _merge_parameter_schema(schema, field_schema)
                    if enum := _schema_enum(field_schema):
                        parameter["enum"] = list(
                            dict.fromkeys([*parameter.get("enum", ()), *enum]),
                        )

                for attribute in ("description", "examples"):
                    value = getattr(custom, attribute, None)
                    if value and attribute not in parameter:
                        parameter[attribute] = value

                if custom.default is not EMPTY and "default" not in parameter:
                    parameter["default"] = custom.default
                    parameter.setdefault("schema", {}).setdefault(
                        "default",
                        custom.default,
                    )

        return parameters

    def get_payloads(self) -> list[tuple["dict[str, Any]", str]]:
        payloads: list[tuple[dict[str, Any], str]] = []

        call_name = self.call_name

        for h in self.calls:
            if h.dependant is None:
                msg = "You should setup `Handler` at first."
                raise SetupError(msg)

            body = parse_handler_params(
                h.dependant,
                prefix=f"{self.config.title_ or call_name}:Message",
            )
            payloads.append((body, to_camelcase(h.name)))

        if not self.calls:
            payloads.append(
                (
                    {
                        "title": f"{self.config.title_ or call_name}:Message:Payload",
                    },
                    to_camelcase(call_name),
                ),
            )

        return payloads

    @property
    @abstractmethod
    def channel_labels(self) -> list[str]:
        """The label each of this endpoint's channels is keyed by, one per channel.

        Usually the address, but not always: MQTT keeps the `$share/` prefix its
        address drops, and RabbitMQ names a queue and an exchange rather than the
        routing key a message travels by.
        """
        raise NotImplementedError

    @property
    def name(self) -> str:
        """The key of the first channel this endpoint names."""
        labels = self.channel_labels
        return self._channel_key(labels[0], split=len(labels) > 1)

    @abstractmethod
    def get_schema(self) -> dict[str, "SubscriberSpec"]:
        raise NotImplementedError
