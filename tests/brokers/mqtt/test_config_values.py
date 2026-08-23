import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest

from faststream import Config
from faststream.exceptions import SetupError
from faststream.params import Path
from tests.brokers.base.config import ConfigOverrideTestcase

from .basic import MQTTMemoryTestcaseConfig


@pytest.mark.mqtt()
class TestConfigValues(MQTTMemoryTestcaseConfig, ConfigOverrideTestcase):
    log_context_address_key = "topic"

    # MQTT is fire-and-forget: a Publisher has no reply destination.
    supports_reply_to = False

    @pytest.mark.asyncio()
    async def test_shared_value(self, queue: str, mock: MagicMock) -> None:
        """Two subscribers land in one shared group, so only one of them eats."""
        broker = self.get_broker(config_values={"GROUP": f"{queue}-group"})

        @broker.subscriber(queue, shared=Config("GROUP"))
        async def resolved(msg: Any) -> None:
            mock("resolved")

        @broker.subscriber(queue, shared=f"{queue}-group")
        async def literal(msg: Any) -> None:
            mock("literal")

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", queue)

        # Which of the two eats depends on iteration order; that only one does
        # is the assertion — an unresolved placeholder is a group of its own.
        assert mock.call_count == 1, mock.call_args_list

    @pytest.mark.asyncio()
    async def test_topic_and_shared_values_together(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        broker = self.get_broker(config_values={"IN": queue, "GROUP": f"{queue}-group"})

        @broker.subscriber(Config("IN"), shared=Config("GROUP"))
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await self.assert_consume(br, queue, event)

    @pytest.mark.asyncio()
    async def test_a_config_value_holding_an_address_template_fills_path(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """A Config value is read as an Address template, exactly as a literal is."""
        broker = self.get_broker(
            apply_types=True,
            config_values={"IN": f"{queue}/{{level}}"},
        )

        @broker.subscriber(Config("IN"))
        async def handler(msg: Any, level: str = Path()) -> None:
            mock(level)

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", f"{queue}/info")

        mock.assert_called_once_with("info")

    @pytest.mark.asyncio()
    async def test_an_escaped_brace_in_a_config_value_stays_a_literal_brace(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """MQTT's `{{`/`}}` escape survives arriving from a Config value.

        It must neither be read as a Path parameter nor be caught by the check
        that refuses a resolved value whose braces are not a template.
        """
        broker = self.get_broker(
            apply_types=True,
            config_values={"IN": f"{queue}/{{{{raw}}}}/{{level}}"},
        )

        @broker.subscriber(Config("IN"))
        async def handler(msg: Any, level: str = Path()) -> None:
            mock(level)

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", f"{queue}/{{raw}}/info")

        mock.assert_called_once_with("info")

    @pytest.mark.asyncio()
    async def test_an_unsatisfiable_path_names_the_config_key(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True, config_values={"IN": queue})

        @broker.subscriber(Config("IN"))
        async def handler(msg: Any, level: str = Path()) -> None: ...

        with pytest.raises(SetupError, match=r"Config value 'IN'"):
            async with self.patch_broker(broker):
                pass

    @pytest.mark.asyncio()
    async def test_a_config_value_that_is_not_a_template_names_the_config_key(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker(config_values={"IN": f"{queue}/${{ENV"})

        @broker.subscriber(Config("IN"))
        async def handler(msg: Any) -> None: ...

        with pytest.raises(SetupError, match=r"Config value 'IN'"):
            async with self.patch_broker(broker):
                pass
