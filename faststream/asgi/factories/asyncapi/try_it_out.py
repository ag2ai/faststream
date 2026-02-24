from collections.abc import Sequence
from contextlib import suppress
from functools import lru_cache
from typing import TYPE_CHECKING, Any, TypedDict, Union

from faststream.asgi.annotations import Request
from faststream.asgi.handlers import PostHandler, post
from faststream.asgi.response import AsgiResponse, JSONResponse
from faststream.exceptions import SubscriberNotFound

if TYPE_CHECKING:
    from faststream._internal.broker import BrokerUsecase
    from faststream._internal.testing.broker import TestBroker
    from faststream.specification.schema import Tag, TagDict


class TryItOutOptions(TypedDict, total=False):
    sendToRealBroker: bool
    timestamp: str


class TryItOutForm(TypedDict):
    channelName: str
    message: dict[str, Any]
    options: TryItOutOptions


class TryItOutProcessor:
    """Process try-it-out requests: parse, validate, publish to real or test broker."""

    def __init__(self, broker: "BrokerUsecase[Any, Any]") -> None:
        self._broker = broker

        registry = _get_broker_registry()
        for br_cls, test_broker_cls in registry.items():
            if isinstance(self._broker, br_cls):
                self._test_broker = test_broker_cls(broker)
                break

        else:
            msg = f"TestBroker not available for {broker}. Please, inspect your dependencies."
            raise ValueError(msg)

    async def process(self, body: TryItOutForm) -> AsgiResponse:
        """Process parsed body: validate, dry-run or publish. Returns response."""
        destination, *_ = body.get("channelName", "").split(":")

        if not destination:
            return JSONResponse({"details": "Missing channelName"}, 400)

        payload = body.get("message", {})
        options = body.get("options", {})
        use_real_broker = options.get("sendToRealBroker", False)

        result = ""
        try:
            if use_real_broker:
                await self._broker.publish(payload, destination)

            else:
                async with self._test_broker as br:
                    data = await br.request(payload, destination)
                    with suppress(Exception):
                        result = data.body.decode()

        except SubscriberNotFound:
            return JSONResponse({"details": f"{destination} destination not found."}, 404)

        except Exception as e:
            return JSONResponse({"details": str(e)}, 500)

        mode = "real" if use_real_broker else "test"
        answer = {"status": "ok", "mode": mode}
        if result:
            answer["result"] = result
        return JSONResponse(answer, 200)


def make_try_it_out_handler(
    broker: "BrokerUsecase[Any, Any]",
    description: str | None = None,
    tags: Sequence[Union["Tag", "TagDict", dict[str, Any]]] | None = None,
    unique_id: str | None = None,
    include_in_schema: bool = False,
) -> "PostHandler":
    """Create POST handler for asyncapi-try-it-plugin to publish messages to broker."""
    processor = TryItOutProcessor(broker)

    @post(
        description=description,
        tags=tags,
        unique_id=unique_id,
        include_in_schema=include_in_schema,
    )
    async def try_it_out(request: Request) -> AsgiResponse:
        try:
            body: TryItOutForm = await request.json()

        except Exception as e:
            return JSONResponse({"details": f"Invalid JSON: {e}"}, 400)

        return await processor.process(body)

    return try_it_out


@lru_cache(maxsize=1)
def _get_broker_registry() -> dict[
    type["BrokerUsecase[Any, Any]"],
    type["TestBroker[Any]"],
]:
    registry: dict[type[BrokerUsecase[Any, Any]], type[TestBroker[Any]]] = {}

    with suppress(ImportError):
        from faststream.confluent import (
            KafkaBroker as ConfluentKafkaBroker,
            TestKafkaBroker as TestConfluentKafkaBroker,
        )

        registry[ConfluentKafkaBroker] = TestConfluentKafkaBroker

    with suppress(ImportError):
        from faststream.kafka import (
            KafkaBroker as AioKafkaBroker,
            TestKafkaBroker as TestAioKafkaBroker,
        )

        registry[AioKafkaBroker] = TestAioKafkaBroker

    with suppress(ImportError):
        from faststream.nats import NatsBroker, TestNatsBroker

        registry[NatsBroker] = TestNatsBroker

    with suppress(ImportError):
        from faststream.rabbit import RabbitBroker, TestRabbitBroker

        registry[RabbitBroker] = TestRabbitBroker

    with suppress(ImportError):
        from faststream.redis import RedisBroker, TestRedisBroker

        registry[RedisBroker] = TestRedisBroker

    return registry
