import pytest

from faststream._internal.utils.path import Address, AddressSyntax
from tests.brokers.confluent.basic import ConfluentTestcaseConfig

CONFLUENT_SYNTAX = AddressSyntax(
    replace_symbol="*",
    patch_regex=lambda x: x,
)


@pytest.mark.confluent()
class TestConfluentAddressTemplate(ConfluentTestcaseConfig):
    def test_escaped_braces_are_literal(self) -> None:
        address = Address("cache{{shard}}", CONFLUENT_SYNTAX)

        assert address.template == "cache{shard}"
        assert address.broker_address == "cache{shard}"
        assert address.regex is None

    def test_escaped_braces_with_parameters(self) -> None:
        address = Address("cache{{shard}}.logs.{level}", CONFLUENT_SYNTAX)

        assert address.template == "cache{shard}.logs.{level}"
        assert address.broker_address == "cache{shard}.logs.*"
        assert address.regex is not None
