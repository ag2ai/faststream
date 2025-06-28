from dirty_equals import IsStr

from faststream._internal.broker import BrokerUsecase
from faststream._internal.broker.router import (
    ArgsContainer,
    BrokerRouter,
    SubscriberRoute,
)

from .basic import AsyncAPI300Factory


class RouterTestcase(AsyncAPI300Factory):
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

        schema = self.get_spec(broker).to_jsonable()

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

        schema = self.get_spec(broker).to_jsonable()
        schemas = schema["components"]["schemas"]
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

        schema = self.get_spec(broker).to_jsonable()
        assert schema["channels"] == {}, schema["channels"]

    def test_not_include_in_method(self) -> None:
        broker = self.broker_class()
        router = self.router_class()

        @router.subscriber("test")
        @router.publisher("test")
        async def handle(msg) -> None: ...

        broker.include_router(router, include_in_schema=False)

        schema = self.get_spec(broker).to_jsonable()
        assert schema["channels"] == {}, schema["channels"]

    def test_respect_subrouter(self) -> None:
        broker = self.broker_class()
        router = self.router_class()
        router2 = self.router_class(include_in_schema=False)

        @router2.subscriber("test")
        @router2.publisher("test")
        async def handle(msg) -> None: ...

        router.include_router(router2)
        broker.include_router(router)

        schema = self.get_spec(broker).to_jsonable()

        assert schema["channels"] == {}, schema["channels"]

    def test_not_include_subrouter(self) -> None:
        broker = self.broker_class()
        router = self.router_class(include_in_schema=False)
        router2 = self.router_class()

        @router2.subscriber("test")
        @router2.publisher("test")
        async def handle(msg) -> None: ...

        router.include_router(router2)
        broker.include_router(router)

        schema = self.get_spec(broker).to_jsonable()

        assert schema["channels"] == {}

    def test_not_include_subrouter_by_method(self) -> None:
        broker = self.broker_class()
        router = self.router_class()
        router2 = self.router_class()

        @router2.subscriber("test")
        @router2.publisher("test")
        async def handle(msg) -> None: ...

        router.include_router(router2, include_in_schema=False)
        broker.include_router(router)

        schema = self.get_spec(broker).to_jsonable()

        assert schema["channels"] == {}

    def test_all_nested_routers_by_method(self) -> None:
        broker = self.broker_class()
        router = self.router_class()
        router2 = self.router_class()

        @router2.subscriber("test")
        @router2.publisher("test")
        async def handle(msg) -> None: ...

        router.include_router(router2)
        broker.include_router(router, include_in_schema=False)

        schema = self.get_spec(broker).to_jsonable()

        assert schema["channels"] == {}

    def test_include_subrouter(self) -> None:
        broker = self.broker_class()
        router = self.router_class()
        router2 = self.router_class()

        @router2.subscriber("test")
        @router2.publisher("test")
        async def handle(msg) -> None: ...

        router.include_router(router2)
        broker.include_router(router)

        schema = self.get_spec(broker).to_jsonable()

        assert len(schema["channels"]) == 2
