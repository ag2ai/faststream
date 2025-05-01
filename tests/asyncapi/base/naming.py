from typing import Any, Type

from dirty_equals import Contains, IsStr
from pydantic import create_model

from faststream import FastStream
from faststream.asyncapi.generate import get_app_schema
from faststream.broker.core.usecase import BrokerUsecase


class BaseNaming:
    broker_class: Type[BrokerUsecase[Any, Any]]


class SubscriberNaming(BaseNaming):
    def test_subscriber_naming(self):
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle_user_created(msg: str): ...

        schema = get_app_schema(FastStream(broker)).to_jsonable()

        assert list(schema["channels"].keys()) == [
            IsStr(regex=r"test[\w:]*:HandleUserCreated")
        ]

        assert list(schema["components"]["messages"].keys()) == [
            IsStr(regex=r"test[\w:]*:HandleUserCreated:Message")
        ]

        assert list(schema["components"]["schemas"].keys()) == [
            "HandleUserCreated:Message:Payload"
        ]

    def test_pydantic_subscriber_naming(self):
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle_user_created(msg: create_model("SimpleModel")): ...

        schema = get_app_schema(FastStream(broker)).to_jsonable()

        assert list(schema["channels"].keys()) == [
            IsStr(regex=r"test[\w:]*:HandleUserCreated")
        ]

        assert list(schema["components"]["messages"].keys()) == [
            IsStr(regex=r"test[\w:]*:HandleUserCreated:Message")
        ]

        assert list(schema["components"]["schemas"].keys()) == ["SimpleModel"]

    def test_multi_subscribers_naming(self):
        broker = self.broker_class()

        @broker.subscriber("test")
        @broker.subscriber("test2")
        async def handle_user_created(msg: str): ...

        schema = get_app_schema(FastStream(broker)).to_jsonable()

        assert list(schema["channels"].keys()) == [
            IsStr(regex=r"test[\w:]*:HandleUserCreated"),
            IsStr(regex=r"test2[\w:]*:HandleUserCreated"),
        ]

        assert list(schema["components"]["messages"].keys()) == [
            IsStr(regex=r"test[\w:]*:HandleUserCreated:Message"),
            IsStr(regex=r"test2[\w:]*:HandleUserCreated:Message"),
        ]

        assert list(schema["components"]["schemas"].keys()) == [
            "HandleUserCreated:Message:Payload"
        ]

    def test_subscriber_naming_manual(self):
        broker = self.broker_class()

        @broker.subscriber("test", title="custom")
        async def handle_user_created(msg: str): ...

        schema = get_app_schema(FastStream(broker)).to_jsonable()

        assert list(schema["channels"].keys()) == ["custom"]

        assert list(schema["components"]["messages"].keys()) == ["custom:Message"]

        assert list(schema["components"]["schemas"].keys()) == [
            "custom:Message:Payload"
        ]

    def test_subscriber_naming_default(self):
        broker = self.broker_class()

        broker.subscriber("test")

        schema = get_app_schema(FastStream(broker)).to_jsonable()

        assert list(schema["channels"].keys()) == [
            IsStr(regex=r"test[\w:]*:Subscriber")
        ]

        assert list(schema["components"]["messages"].keys()) == [
            IsStr(regex=r"test[\w:]*:Subscriber:Message")
        ]

        for key, v in schema["components"]["schemas"].items():
            assert key == "Subscriber:Message:Payload"
            assert v == {"title": key}

    def test_subscriber_naming_default_with_title(self):
        broker = self.broker_class()

        broker.subscriber("test", title="custom")

        schema = get_app_schema(FastStream(broker)).to_jsonable()

        assert list(schema["channels"].keys()) == ["custom"]

        assert list(schema["components"]["messages"].keys()) == ["custom:Message"]

        assert list(schema["components"]["schemas"].keys()) == [
            "custom:Message:Payload"
        ]

        assert schema["components"]["schemas"]["custom:Message:Payload"] == {
            "title": "custom:Message:Payload"
        }

    def test_multi_subscribers_naming_default(self):
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle_user_created(msg: str): ...

        broker.subscriber("test2")
        broker.subscriber("test3")

        schema = get_app_schema(FastStream(broker)).to_jsonable()

        assert list(schema["channels"].keys()) == [
            IsStr(regex=r"test[\w:]*:HandleUserCreated"),
            IsStr(regex=r"test2[\w:]*:Subscriber"),
            IsStr(regex=r"test3[\w:]*:Subscriber"),
        ]

        assert list(schema["components"]["messages"].keys()) == [
            IsStr(regex=r"test[\w:]*:HandleUserCreated:Message"),
            IsStr(regex=r"test2[\w:]*:Subscriber:Message"),
            IsStr(regex=r"test3[\w:]*:Subscriber:Message"),
        ]

        assert list(schema["components"]["schemas"].keys()) == [
            "HandleUserCreated:Message:Payload",
            "Subscriber:Message:Payload",
        ]

        assert schema["components"]["schemas"]["Subscriber:Message:Payload"] == {
            "title": "Subscriber:Message:Payload"
        }


class FilterNaming(BaseNaming):
    def test_subscriber_filter_base(self):
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle_user_created(msg: str): ...

        @broker.subscriber("test")
        async def handle_user_id(msg: int): ...

        schema = get_app_schema(FastStream(broker)).to_jsonable()

        assert list(schema["channels"].keys()) == [
            IsStr(regex=r"test[\w:]*:\[HandleUserCreated,HandleUserId\]")
        ]

        assert list(schema["components"]["messages"].keys()) == [
            IsStr(regex=r"test[\w:]*:\[HandleUserCreated,HandleUserId\]:Message")
        ]

        assert list(schema["components"]["schemas"].keys()) == [
            "HandleUserCreated:Message:Payload",
            "HandleUserId:Message:Payload",
        ]

    def test_subscriber_filter_pydantic(self):
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle_user_created(msg: create_model("SimpleModel")): ...

        @broker.subscriber("test")
        async def handle_user_id(msg: int): ...

        schema = get_app_schema(FastStream(broker)).to_jsonable()

        assert list(schema["channels"].keys()) == [
            IsStr(regex=r"test[\w:]*:\[HandleUserCreated,HandleUserId\]")
        ]

        assert list(schema["components"]["messages"].keys()) == [
            IsStr(regex=r"test[\w:]*:\[HandleUserCreated,HandleUserId\]:Message")
        ]

        assert list(schema["components"]["schemas"].keys()) == [
            "SimpleModel",
            "HandleUserId:Message:Payload",
        ]

    def test_subscriber_filter_with_title(self):
        broker = self.broker_class()

        @broker.subscriber("test", title="custom")
        async def handle_user_created(msg: str): ...

        @broker.subscriber("test", title="custom")
        async def handle_user_id(msg: int): ...

        schema = get_app_schema(FastStream(broker)).to_jsonable()

        assert list(schema["channels"].keys()) == ["custom"]

        assert list(schema["components"]["messages"].keys()) == ["custom:Message"]

        assert list(schema["components"]["schemas"].keys()) == [
            "HandleUserCreated:Message:Payload",
            "HandleUserId:Message:Payload",
        ]


class PublisherNaming(BaseNaming):
    def test_publisher_naming_base(self):
        broker = self.broker_class()

        @broker.publisher("test")
        async def handle_user_created() -> str: ...

        schema = get_app_schema(FastStream(broker)).to_jsonable()

        assert list(schema["channels"].keys()) == [IsStr(regex=r"test[\w:]*:Publisher")]

        assert list(schema["components"]["messages"].keys()) == [
            IsStr(regex=r"test[\w:]*:Publisher:Message")
        ]

        assert list(schema["components"]["schemas"].keys()) == [
            IsStr(regex=r"test[\w:]*:Publisher:Message:Payload")
        ]

    def test_publisher_naming_pydantic(self):
        broker = self.broker_class()

        @broker.publisher("test")
        async def handle_user_created() -> create_model("SimpleModel"): ...

        schema = get_app_schema(FastStream(broker)).to_jsonable()

        assert list(schema["channels"].keys()) == [IsStr(regex=r"test[\w:]*:Publisher")]

        assert list(schema["components"]["messages"].keys()) == [
            IsStr(regex=r"test[\w:]*:Publisher:Message")
        ]

        assert list(schema["components"]["schemas"].keys()) == [
            "SimpleModel",
        ]

    def test_publisher_manual_naming(self):
        broker = self.broker_class()

        @broker.publisher("test", title="custom")
        async def handle_user_created() -> str: ...

        schema = get_app_schema(FastStream(broker)).to_jsonable()

        assert list(schema["channels"].keys()) == ["custom"]

        assert list(schema["components"]["messages"].keys()) == ["custom:Message"]

        assert list(schema["components"]["schemas"].keys()) == [
            "custom:Message:Payload"
        ]

    def test_publisher_with_schema_naming(self):
        broker = self.broker_class()

        @broker.publisher("test", schema=str)
        async def handle_user_created(): ...

        schema = get_app_schema(FastStream(broker)).to_jsonable()

        assert list(schema["channels"].keys()) == [IsStr(regex=r"test[\w:]*:Publisher")]

        assert list(schema["components"]["messages"].keys()) == [
            IsStr(regex=r"test[\w:]*:Publisher:Message")
        ]

        assert list(schema["components"]["schemas"].keys()) == [
            IsStr(regex=r"test[\w:]*:Publisher:Message:Payload")
        ]

    def test_publisher_manual_naming_with_schema(self):
        broker = self.broker_class()

        @broker.publisher("test", title="custom", schema=str)
        async def handle_user_created(): ...

        schema = get_app_schema(FastStream(broker)).to_jsonable()

        assert list(schema["channels"].keys()) == ["custom"]

        assert list(schema["components"]["messages"].keys()) == ["custom:Message"]

        assert list(schema["components"]["schemas"].keys()) == [
            "custom:Message:Payload"
        ]

    def test_multi_publishers_naming(self):
        broker = self.broker_class()

        @broker.publisher("test")
        @broker.publisher("test2")
        async def handle_user_created() -> str: ...

        schema = get_app_schema(FastStream(broker)).to_jsonable()

        names = list(schema["channels"].keys())
        assert names == Contains(
            IsStr(regex=r"test2[\w:]*:Publisher"),
            IsStr(regex=r"test[\w:]*:Publisher"),
        ), names

        messages = list(schema["components"]["messages"].keys())
        assert messages == Contains(
            IsStr(regex=r"test2[\w:]*:Publisher:Message"),
            IsStr(regex=r"test[\w:]*:Publisher:Message"),
        ), messages

        payloads = list(schema["components"]["schemas"].keys())
        assert payloads == Contains(
            IsStr(regex=r"test2[\w:]*:Publisher:Message:Payload"),
            IsStr(regex=r"test[\w:]*:Publisher:Message:Payload"),
        ), payloads

    def test_multi_publisher_usages(self):
        broker = self.broker_class()

        pub = broker.publisher("test")

        @pub
        async def handle_user_created() -> str: ...

        @pub
        async def handle() -> int: ...

        schema = get_app_schema(FastStream(broker)).to_jsonable()

        assert list(schema["channels"].keys()) == [
            IsStr(regex=r"test[\w:]*:Publisher"),
        ]

        assert list(schema["components"]["messages"].keys()) == [
            IsStr(regex=r"test[\w:]*:Publisher:Message"),
        ]

        assert list(schema["components"]["schemas"].keys()) == [
            "HandleUserCreated:Publisher:Message:Payload",
            "Handle:Publisher:Message:Payload",
        ]

    def test_multi_publisher_usages_with_custom(self):
        broker = self.broker_class()

        pub = broker.publisher("test", title="custom")

        @pub
        async def handle_user_created() -> str: ...

        @pub
        async def handle() -> int: ...

        schema = get_app_schema(FastStream(broker)).to_jsonable()

        assert list(schema["channels"].keys()) == ["custom"]

        assert list(schema["components"]["messages"].keys()) == ["custom:Message"]

        assert list(schema["components"]["schemas"].keys()) == [
            "HandleUserCreated:Publisher:Message:Payload",
            "Handle:Publisher:Message:Payload",
        ]


class NamingTestCase(SubscriberNaming, FilterNaming, PublisherNaming):
    pass
