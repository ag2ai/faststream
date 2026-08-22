import asyncio
from typing import Any
from unittest.mock import MagicMock

import anyio
import pytest

from faststream import Config
from faststream.exceptions import SetupError
from faststream.params import Path
from faststream.rabbit import ExchangeType, RabbitExchange, RabbitQueue
from tests.brokers.base.config import ConfigOverrideTestcase

from .basic import RabbitMemoryTestcaseConfig


@pytest.mark.rabbit()
class TestConfigValues(RabbitMemoryTestcaseConfig, ConfigOverrideTestcase):
    def get_config_value(self, address: str) -> Any:
        """A prepared `RabbitQueue`, so a whole object as a value is asserted."""
        return RabbitQueue(address)

    @pytest.mark.asyncio()
    async def test_exchange_value(self, queue: str, event: asyncio.Event) -> None:
        """A whole binding is configurable, not half of it (story 6)."""
        exchange = f"{queue}-exchange"
        broker = self.get_broker(config={"EXCHANGE": exchange})

        @broker.subscriber(queue, Config("EXCHANGE"))
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", queue, exchange=exchange)

            with anyio.move_on_after(self.timeout):
                await event.wait()

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_prepared_exchange_object_as_a_value(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        exchange = RabbitExchange(f"{queue}-exchange", type=ExchangeType.FANOUT)
        broker = self.get_broker(config={"EXCHANGE": exchange})

        @broker.subscriber(queue, Config("EXCHANGE"))
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", queue, exchange=exchange)

            with anyio.move_on_after(self.timeout):
                await event.wait()

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_queue_and_exchange_decide_the_prefix_independently(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
        event2: asyncio.Event,
    ) -> None:
        """A literal queue beside a placeholder exchange is still prefixed (ADR-0003)."""
        exchange = f"{queue}-exchange"
        router = self.get_router(prefix="prefix-")

        @router.subscriber(queue, Config("EXCHANGE"))
        async def literal_queue(msg: Any) -> None:
            mock("literal-queue")
            event.set()

        @router.subscriber(Config("IN"), exchange)
        async def literal_exchange(msg: Any) -> None:
            mock("literal-exchange")
            event2.set()

        broker = self.get_broker(
            config={"EXCHANGE": exchange, "IN": f"resolved-{queue}"},
        )
        broker.include_router(router)

        async with self.patch_broker(broker) as br:
            await br.start()

            await br.publish("hello", f"prefix-{queue}", exchange=exchange)
            await br.publish("hello", f"resolved-{queue}", exchange=exchange)

            with anyio.move_on_after(self.timeout):
                await event.wait()
                await event2.wait()

        assert event.is_set()
        assert event2.is_set()
        assert mock.call_count == 2, mock.call_args_list

    @pytest.mark.asyncio()
    async def test_log_line_names_the_resolved_queue(self, queue: str) -> None:
        broker = self.get_broker(config={"IN": queue})

        @broker.subscriber(Config("IN"))
        async def handler(msg: Any) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", queue)

            logger = br.config.logger.logger.logger
            queues = {
                call.kwargs["extra"]["queue"]
                for call in logger.log.call_args_list
                if call.kwargs.get("extra")
            }

        assert queues == {queue}, queues

    @pytest.mark.asyncio()
    async def test_a_config_value_holding_an_address_template_fills_path(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """A Config value is read as an Address template, exactly as a literal is.

        RabbitMQ spells its template in the routing key rather than the queue
        name, so the value carrying one is a prepared `RabbitQueue`.
        """
        exchange = RabbitExchange(f"{queue}-exchange", type=ExchangeType.TOPIC)
        broker = self.get_broker(
            apply_types=True,
            config={"IN": RabbitQueue(queue, routing_key=f"{queue}.{{level}}")},
        )

        @broker.subscriber(Config("IN"), exchange)
        async def handler(msg: Any, level: str = Path()) -> None:
            mock(level)

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", f"{queue}.info", exchange=exchange)

        mock.assert_called_once_with("info")

    @pytest.mark.asyncio()
    async def test_a_brace_in_a_queue_name_is_no_more_a_template_than_a_literal_is(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """A Config value is read exactly as a literal declaration is (ADR-0003).

        RabbitMQ spells its Address template in the routing key, never in the
        queue name, so `{level}` in a name names a queue — both ways round.
        """
        name = f"{queue}.{{level}}"
        broker = self.get_broker(config={"IN": name})

        @broker.subscriber(Config("IN"))
        async def resolved(msg: Any) -> None:
            mock("resolved")

        @broker.subscriber(name)
        async def literal(msg: Any) -> None:
            mock("literal")

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", name)

        assert mock.call_count == 2, mock.call_args_list

    @pytest.mark.asyncio()
    async def test_an_unsatisfiable_path_names_the_config_key(self, queue: str) -> None:
        broker = self.get_broker(
            apply_types=True,
            config={"IN": RabbitQueue(queue, routing_key=f"{queue}.info")},
        )

        @broker.subscriber(Config("IN"))
        async def handler(msg: Any, level: str = Path()) -> None: ...

        with pytest.raises(SetupError, match="Config value 'IN'"):
            async with self.patch_broker(broker):
                pass

    @pytest.mark.asyncio()
    async def test_a_config_value_that_is_not_a_template_names_the_config_key(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker(
            config={"IN": RabbitQueue(queue, routing_key="logs.${ENV")},
        )

        @broker.subscriber(Config("IN"))
        async def handler(msg: Any) -> None: ...

        with pytest.raises(SetupError, match="Config value 'IN'"):
            async with self.patch_broker(broker):
                pass

    @pytest.mark.asyncio()
    async def test_publisher_routing_key_value(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """A Publisher's routing key is configurable, like its queue and exchange."""
        exchange = RabbitExchange(f"{queue}-exchange", type=ExchangeType.TOPIC)
        broker = self.get_broker(config={"RK": f"{queue}.info"})

        publisher = broker.publisher(exchange=exchange, routing_key=Config("RK"))

        @broker.subscriber(RabbitQueue(queue, routing_key=f"{queue}.*"), exchange)
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await publisher.publish("hello")

            with anyio.move_on_after(self.timeout):
                await event.wait()

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_publisher_routing_key_decides_the_prefix_independently(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
        event2: asyncio.Event,
    ) -> None:
        """A resolved routing key is undecorated; a literal one beside it is not (ADR-0003)."""
        exchange = RabbitExchange(f"{queue}-exchange", type=ExchangeType.TOPIC)
        router = self.get_router(prefix="prefix-")

        resolved_publisher = router.publisher(
            exchange=exchange,
            routing_key=Config("RK"),
        )
        literal_publisher = router.publisher(
            exchange=exchange,
            routing_key=f"literal-{queue}",
        )

        broker = self.get_broker(config={"RK": queue})

        @broker.subscriber(RabbitQueue(f"{queue}-in", routing_key=queue), exchange)
        async def resolved(msg: Any) -> None:
            mock("resolved")
            event.set()

        @broker.subscriber(
            RabbitQueue(f"{queue}-in2", routing_key=f"prefix-literal-{queue}"),
            exchange,
        )
        async def literal(msg: Any) -> None:
            mock("literal")
            event2.set()

        broker.include_router(router)

        async with self.patch_broker(broker) as br:
            await br.start()

            await resolved_publisher.publish("hello")
            await literal_publisher.publish("hello")

            with anyio.move_on_after(self.timeout):
                await event.wait()
                await event2.wait()

        assert event.is_set()
        assert event2.is_set()
        assert mock.call_count == 2, mock.call_args_list

    @pytest.mark.asyncio()
    async def test_publisher_reply_to_value(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """A Publisher's reply destination is configurable too (ADR-0002)."""
        out_queue = f"{queue}-out"
        reply_queue = f"{queue}-reply"

        broker = self.get_broker(config={"REPLY": reply_queue})

        @broker.subscriber(queue)
        @broker.publisher(out_queue, reply_to=Config("REPLY"))
        async def handler(msg: Any) -> str:
            return "response"

        @broker.subscriber(out_queue)
        async def out(msg: Any) -> str:
            return "reply"

        @broker.subscriber(reply_queue)
        async def reply(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", queue)

            with anyio.move_on_after(self.timeout):
                await event.wait()

        assert event.is_set()
