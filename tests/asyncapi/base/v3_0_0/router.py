from dirty_equals import IsStr

from faststream._internal.broker.broker import BrokerUsecase
from faststream._internal.broker.router import (
    ArgsContainer,
    BrokerRouter,
    SubscriberRoute,
)
from faststream.specification.asyncapi import AsyncAPI


class RouterTestcase:
    broker_class: type[BrokerUsecase]
    router_class: type[BrokerRouter]
    publisher_class: type[ArgsContainer]
    route_class: type[SubscriberRoute]

    def test_delay_subscriber(self) -> None:
        broker = self.broker_class()

        async def handle(msg) -> None: ...

        router = self.router_class(
            handlers=(self.route_class(handle, "test"),),
        )

        broker.include_router(router)

        schema = AsyncAPI(broker, schema_version="3.0.0").to_jsonable()

        payload = schema["components"]["schemas"]
        key = list(payload.keys())[0]  # noqa: RUF015
        assert payload[key]["title"] == key == "Handle:Message:Payload"

    def test_delay_publisher(self) -> None:
        broker = self.broker_class()

        async def handle(msg) -> None: ...

        router = self.router_class(
            handlers=(
                self.route_class(
                    handle,
                    "test",
                    publishers=(self.publisher_class("test2", schema=int),),
                ),
            ),
        )

        broker.include_router(router)

        schema = AsyncAPI(broker, schema_version="3.0.0")
        schemas = schema.to_jsonable()["components"]["schemas"]
        del schemas["Handle:Message:Payload"]

        for i, j in schemas.items():
            assert (
                i == j["title"] == IsStr(regex=r"test2[\w:]*:Publisher:Message:Payload")
            )
            assert j["type"] == "integer"

    def test_not_include(self) -> None:
        broker = self.broker_class()
        router = self.router_class(include_in_schema=False)

        @router.subscriber("test")
        @router.publisher("test")
        async def handle(msg) -> None: ...

        broker.include_router(router)

        schema = AsyncAPI(broker, schema_version="3.0.0")
        assert schema.to_jsonable()["channels"] == {}, schema.to_jsonable()["channels"]

    def test_not_include_in_method(self) -> None:
        broker = self.broker_class()
        router = self.router_class()

        @router.subscriber("test")
        @router.publisher("test")
        async def handle(msg) -> None: ...

        broker.include_router(router, include_in_schema=False)

        schema = AsyncAPI(broker, schema_version="3.0.0")
        assert schema.to_jsonable()["channels"] == {}, schema.to_jsonable()["channels"]

    def test_respect_subrouter(self) -> None:
        broker = self.broker_class()
        router = self.router_class()
        router2 = self.router_class(include_in_schema=False)

        @router2.subscriber("test")
        @router2.publisher("test")
        async def handle(msg) -> None: ...

        router.include_router(router2)
        broker.include_router(router)

        schema = AsyncAPI(broker, schema_version="3.0.0")

        assert schema.to_jsonable()["channels"] == {}, schema.to_jsonable()["channels"]

    def test_not_include_subrouter(self) -> None:
        broker = self.broker_class()
        router = self.router_class(include_in_schema=False)
        router2 = self.router_class()

        @router2.subscriber("test")
        @router2.publisher("test")
        async def handle(msg) -> None: ...

        router.include_router(router2)
        broker.include_router(router)

        schema = AsyncAPI(broker, schema_version="3.0.0")

        assert schema.to_jsonable()["channels"] == {}

    def test_not_include_subrouter_by_method(self) -> None:
        broker = self.broker_class()
        router = self.router_class()
        router2 = self.router_class()

        @router2.subscriber("test")
        @router2.publisher("test")
        async def handle(msg) -> None: ...

        router.include_router(router2, include_in_schema=False)
        broker.include_router(router)

        schema = AsyncAPI(broker, schema_version="3.0.0")

        assert schema.to_jsonable()["channels"] == {}

    def test_all_nested_routers_by_method(self) -> None:
        broker = self.broker_class()
        router = self.router_class()
        router2 = self.router_class()

        @router2.subscriber("test")
        @router2.publisher("test")
        async def handle(msg) -> None: ...

        router.include_router(router2)
        broker.include_router(router, include_in_schema=False)

        schema = AsyncAPI(broker, schema_version="3.0.0")

        assert schema.to_jsonable()["channels"] == {}

    def test_include_subrouter(self) -> None:
        broker = self.broker_class()
        router = self.router_class()
        router2 = self.router_class()

        @router2.subscriber("test")
        @router2.publisher("test")
        async def handle(msg) -> None: ...

        router.include_router(router2)
        broker.include_router(router)

        schema = AsyncAPI(broker, schema_version="3.0.0")

        assert len(schema.to_jsonable()["channels"]) == 2
