from typing import Any
from unittest.mock import MagicMock

import pytest
from typing_extensions import override

from faststream import Config
from faststream.confluent import KafkaBroker
from tests.brokers.base.config import ConfigOverrideTestcase

from .basic import ConfluentMemoryTestcaseConfig


@pytest.mark.confluent()
class TestConfigValues(ConfluentMemoryTestcaseConfig, ConfigOverrideTestcase):
    log_context_address_key = "topic"

    @override
    def get_subscriber_params(
        self,
        *topics: Any,
        **kwargs: Any,
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        """Declare against `topics`, not `partitions`.

        Confluent's shared hook turns a single address into a `TopicPartition`
        so that a real broker replays from the start. A partition is not an
        address parameter — the option takes structures rather than names and
        stays out of the allowlist — so the placeholder would have nowhere to go.
        """
        return topics, {"auto_offset_reset": "earliest", **kwargs}

    @pytest.mark.asyncio()
    async def test_group_id_value(self, queue: str, mock: MagicMock) -> None:
        """Two subscribers land in one consumer group, so only one of them eats."""
        broker = self.get_broker(config_values={"GROUP": f"{queue}-group"})

        @broker.subscriber(queue, group_id=Config("GROUP"))
        async def resolved(msg: Any) -> None:
            mock("resolved")

        @broker.subscriber(queue, group_id=f"{queue}-group")
        async def literal(msg: Any) -> None:
            mock("literal")

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", queue)

        # Which of the two eats depends on iteration order; that only one does
        # is the assertion — an unresolved placeholder is a group of its own.
        assert mock.call_count == 1, mock.call_args_list

    @pytest.mark.asyncio()
    async def test_none_default_leaves_the_subscriber_without_a_group(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """`None` is a real default: no group, so neither subscriber is deduped."""
        broker = self.get_broker()

        @broker.subscriber(queue, group_id=Config("ABSENT", default=None))
        async def first(msg: Any) -> None:
            mock("first")

        @broker.subscriber(queue, group_id=Config("ABSENT", default=None))
        async def second(msg: Any) -> None:
            mock("second")

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", queue)

        assert mock.call_count == 2, mock.call_args_list


@pytest.mark.confluent()
def test_client_settings_still_reach_the_client() -> None:
    broker = KafkaBroker(config={"message.max.bytes": 1000})

    assert broker.config.connection_config.consumer_config["message.max.bytes"] == 1000
