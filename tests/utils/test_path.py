from importlib import import_module

import pytest

from faststream._internal.utils.path import Address, AddressSyntax, compile_path
from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_mqtt,
    require_nats,
    require_redis,
)

SYNTAX = AddressSyntax(
    replace_symbol="*",
    patch_regex=lambda x: x.replace(r"\*", ".*"),
)


@pytest.mark.parametrize(
    ("module", "name", "declaration"),
    (
        pytest.param(
            "faststream.kafka.subscriber.usecase",
            "KAFKA_ADDRESS_SYNTAX",
            "logs.{{2}}.{level}",
            marks=require_aiokafka,
            id="kafka",
        ),
        pytest.param(
            "faststream.nats.schemas.js_stream",
            "NATS_ADDRESS_SYNTAX",
            "logs.{{2}}.{level}",
            marks=require_nats,
            id="nats",
        ),
        pytest.param(
            "faststream.rabbit.schemas.queue",
            "RABBIT_ADDRESS_SYNTAX",
            "logs.{{2}}.{level}",
            marks=require_aiopika,
            id="rabbit",
        ),
        pytest.param(
            "faststream.redis.schemas.pub_sub",
            "REDIS_ADDRESS_SYNTAX",
            "logs.{{2}}.{level}",
            marks=require_redis,
            id="redis",
        ),
        pytest.param(
            "faststream.mqtt.path",
            "MQTT_ADDRESS_SYNTAX",
            "logs/{{2}}/{level}",
            marks=require_mqtt,
            id="mqtt",
        ),
    ),
)
def test_a_syntax_compiles_the_way_the_module_function_does(
    module: str,
    name: str,
    declaration: str,
) -> None:
    syntax: AddressSyntax = getattr(import_module(module), name)

    assert syntax.compile(declaration) == compile_path(
        declaration,
        replace_symbol=syntax.replace_symbol,
        patch_regex=syntax.patch_regex,
        param_regex=syntax.param_regex,
    )


def test_a_literal_address_is_read_as_characters_rather_than_a_template() -> None:
    address = Address.literal("logs.{{2}}.{level}")

    assert (address.template, address.broker_address, address.regex) == (
        "logs.{{2}}.{level}",
        "logs.{{2}}.{level}",
        None,
    )


def test_a_router_prefix_leaves_a_literal_address_literal() -> None:
    address = Address.literal("logs.{{2}}.{level}").add_prefix("prefix_")

    assert (address.template, address.broker_address, address.regex) == (
        "prefix_logs.{{2}}.{level}",
        "prefix_logs.{{2}}.{level}",
        None,
    )


def test_describe_names_the_template_as_it_was_declared() -> None:
    assert Address("logs.{level}", SYNTAX).describe() == "'logs.{level}'"
