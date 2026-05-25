from typing import Any
from urllib.parse import quote

import pytest
from dirty_equals import Contains, HasLen, IsPartialDict, IsStr
from pydantic import create_model

from faststream._internal.broker import BrokerUsecase

from .basic import AsyncAPI300Factory


class BaseNaming(AsyncAPI300Factory):
    broker_class: type[BrokerUsecase[Any, Any]]


class MultibrokersSchema(BaseNaming):
    def test_multi_shared_server(self, recwarn: pytest.WarningsRecorder) -> None:
        schema = self.get_spec(
            self.broker_class(),
            self.broker_class(),
        ).to_jsonable()

        # assert no warning raised
        assert len(recwarn.list) == 0, [w.message for w in recwarn.list]

        assert schema == IsPartialDict({
            "servers": {"development": IsPartialDict()},
        })

    def test_multi_different_servers(self, recwarn: pytest.WarningsRecorder) -> None:
        schema = self.get_spec(
            self.broker_class(description="broker1"),
            self.broker_class(description="broker2"),
        ).to_jsonable()

        # assert no warning raised
        assert len(recwarn.list) == 0, [w.message for w in recwarn.list]

        assert schema == IsPartialDict({
            "servers": {
                "Server1": IsPartialDict({"description": "broker1"}),
                "Server2": IsPartialDict({"description": "broker2"}),
            },
        })

    def test_multi_subscribers_same_channel(
        self, recwarn: pytest.WarningsRecorder
    ) -> None:
        broker_first = self.broker_class()

        @broker_first.subscriber("test")
        async def handle_broker_first() -> None: ...

        broker_second = self.broker_class()

        @broker_second.subscriber("test")
        async def handle_broker_second() -> None: ...

        schema = self.get_spec(broker_first, broker_second).to_jsonable()

        # assert no warning raised
        assert len(recwarn.list) == 0, [w.message for w in recwarn.list]

        assert list(schema["channels"].items()) == [
            (
                IsStr(regex=r"test[\w:]*:HandleBrokerFirst"),
                IsPartialDict({"servers": [{"$ref": "#/servers/development"}]}),
            ),
            (
                IsStr(regex=r"test[\w:]*:HandleBrokerSecond"),
                IsPartialDict({"servers": [{"$ref": "#/servers/development"}]}),
            ),
        ]

    def test_multi_subscribers_same_channel_different_servers(
        self, recwarn: pytest.WarningsRecorder
    ) -> None:
        broker_first = self.broker_class(description="1")

        @broker_first.subscriber("test")
        async def handle_broker_first() -> None: ...

        broker_second = self.broker_class(description="2")

        @broker_second.subscriber("test")
        async def handle_broker_second() -> None: ...

        schema = self.get_spec(broker_first, broker_second).to_jsonable()

        # assert no warning raised
        assert len(recwarn.list) == 0, [w.message for w in recwarn.list]

        assert list(schema["channels"].items()) == [
            (
                IsStr(regex=r"test[\w:]*:HandleBrokerFirst"),
                IsPartialDict({"servers": [{"$ref": "#/servers/Server1"}]}),
            ),
            (
                IsStr(regex=r"test[\w:]*:HandleBrokerSecond"),
                IsPartialDict({"servers": [{"$ref": "#/servers/Server2"}]}),
            ),
        ]

    def test_multi_shared_pydantic_subscriber_payload(
        self, recwarn: pytest.WarningsRecorder
    ) -> None:
        broker_first = self.broker_class(description="1")

        model = create_model("SimpleModel")

        @broker_first.subscriber("test")
        async def handle_broker_first(msg: model) -> None: ...

        broker_second = self.broker_class(description="2")

        @broker_second.subscriber("test2")
        async def handle_broker_second(msg: model) -> None: ...

        schema = self.get_spec(broker_first, broker_second).to_jsonable()

        # assert no warning raised
        assert len(recwarn.list) == 0, [w.message for w in recwarn.list]

        assert list(schema["components"]["messages"].keys()) == [
            IsStr(regex=r"test[\w:]*:HandleBrokerFirst:SubscribeMessage"),
            IsStr(regex=r"test2[\w:]*:HandleBrokerSecond:SubscribeMessage"),
        ], schema["components"]["messages"].keys()

        assert schema == IsPartialDict({
            "components": IsPartialDict({
                "schemas": {
                    "SimpleModel": IsPartialDict(),
                }
            })
        })

        # def test_multi_subscribers_conflict(self) -> None:
        broker_first = self.broker_class(description="1")

        @broker_first.subscriber("test")
        async def handle_broker() -> None: ...

        broker_second = self.broker_class(description="2")

        @broker_second.subscriber("test")
        async def handle_broker() -> None: ...  # noqa: F811

        with pytest.warns(RuntimeWarning, match=r"test[\w:]*:HandleBroke"):
            schema = self.get_spec(broker_first, broker_second).to_jsonable()

        assert list(schema["channels"].keys()) == [
            IsStr(regex=r"test[\w:]*:HandleBroker")
        ]


class SubscriberNaming(BaseNaming):
    def test_subscriber_naming(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle_user_created(msg: str) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert list(schema["channels"].keys()) == [
            IsStr(regex=r"test[\w:]*:HandleUserCreated"),
        ]

        assert list(schema["components"]["messages"].keys()) == [
            IsStr(regex=r"test[\w:]*:HandleUserCreated:SubscribeMessage"),
        ]

        assert list(schema["components"]["schemas"].keys()) == [
            "HandleUserCreated:Message:Payload",
        ]

    def test_pydantic_subscriber_naming(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle_user_created(msg: create_model("SimpleModel")) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert list(schema["channels"].keys()) == [
            IsStr(regex=r"test[\w:]*:HandleUserCreated"),
        ]

        assert list(schema["components"]["messages"].keys()) == [
            IsStr(regex=r"test[\w:]*:HandleUserCreated:SubscribeMessage"),
        ]

        assert list(schema["components"]["schemas"].keys()) == ["SimpleModel"]

    def test_multi_subscribers_naming(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        @broker.subscriber("test2")
        async def handle_user_created(msg: str) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert list(schema["channels"].keys()) == Contains(
            IsStr(regex=r"test[\w:]*:HandleUserCreated"),
            IsStr(regex=r"test2[\w:]*:HandleUserCreated"),
        ) & HasLen(2)

        assert list(schema["components"]["messages"].keys()) == Contains(
            IsStr(regex=r"test[\w:]*:HandleUserCreated:SubscribeMessage"),
            IsStr(regex=r"test2[\w:]*:HandleUserCreated:SubscribeMessage"),
        ) & HasLen(2)

        assert list(schema["components"]["schemas"].keys()) == [
            "HandleUserCreated:Message:Payload",
        ]

    def test_subscriber_naming_manual(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test", title="custom")
        async def handle_user_created(msg: str) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert list(schema["channels"].keys()) == ["custom"]

        assert list(schema["components"]["messages"].keys()) == [
            "custom:SubscribeMessage",
        ]

        assert list(schema["components"]["schemas"].keys()) == [
            "custom:Message:Payload",
        ]

    def test_subscriber_naming_default(self) -> None:
        broker = self.broker_class()

        sub = broker.subscriber("test")  # noqa: F841

        schema = self.get_spec(broker).to_jsonable()

        assert list(schema["channels"].keys()) == [
            IsStr(regex=r"test[\w:]*:Subscriber"),
        ]

        assert list(schema["components"]["messages"].keys()) == [
            IsStr(regex=r"test[\w:]*:Subscriber:SubscribeMessage"),
        ]

        for key, v in schema["components"]["schemas"].items():
            assert key == "Subscriber:Message:Payload"
            assert v == {"title": key}

    def test_subscriber_naming_default_with_title(self) -> None:
        broker = self.broker_class()

        sub = broker.subscriber("test", title="custom")  # noqa: F841

        schema = self.get_spec(broker).to_jsonable()

        assert list(schema["channels"].keys()) == ["custom"]

        assert list(schema["components"]["messages"].keys()) == [
            "custom:SubscribeMessage",
        ]

        assert list(schema["components"]["schemas"].keys()) == [
            "custom:Message:Payload",
        ]

        assert schema["components"]["schemas"]["custom:Message:Payload"] == {
            "title": "custom:Message:Payload",
        }

    def test_path_channel_refs_are_uri_encoded(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("/test/topic/{user_id}")
        async def sub(name: str, age: int) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        channel_key = next(iter(schema["channels"]))
        message_name = next(iter(schema["channels"][channel_key]["messages"]))
        message_key = next(iter(schema["components"]["messages"]))
        operation = next(iter(schema["operations"].values()))

        encoded_channel_key = quote(channel_key, safe="~:._-")
        encoded_message_key = quote(message_key, safe="~:._-")

        assert schema["channels"][channel_key]["messages"][message_name] == {
            "$ref": f"#/components/messages/{encoded_message_key}",
        }
        assert operation["channel"] == {"$ref": f"#/channels/{encoded_channel_key}"}
        assert operation["messages"] == [
            {"$ref": f"#/channels/{encoded_channel_key}/messages/{message_name}"},
        ]

    def test_multi_subscribers_naming_default(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle_user_created(msg: str) -> None: ...

        sub1 = broker.subscriber("test2")  # noqa: F841
        sub2 = broker.subscriber("test3")  # noqa: F841

        schema = self.get_spec(broker).to_jsonable()

        assert list(schema["channels"].keys()) == Contains(
            IsStr(regex=r"test[\w:]*:HandleUserCreated"),
            IsStr(regex=r"test2[\w:]*:Subscriber"),
            IsStr(regex=r"test3[\w:]*:Subscriber"),
        ) & HasLen(3)

        assert list(schema["components"]["messages"].keys()) == Contains(
            IsStr(regex=r"test[\w:]*:HandleUserCreated:SubscribeMessage"),
            IsStr(regex=r"test2[\w:]*:Subscriber:SubscribeMessage"),
            IsStr(regex=r"test3[\w:]*:Subscriber:SubscribeMessage"),
        ) & HasLen(3)

        assert list(schema["components"]["schemas"].keys()) == Contains(
            "HandleUserCreated:Message:Payload",
            "Subscriber:Message:Payload",
        ) & HasLen(2)

        assert schema["components"]["schemas"]["Subscriber:Message:Payload"] == {
            "title": "Subscriber:Message:Payload",
        }


class FilterNaming(BaseNaming):
    def test_subscriber_filter_base(self) -> None:
        broker = self.broker_class()

        sub = broker.subscriber("test")

        @sub
        async def handle_user_created(msg: str) -> None: ...

        @sub
        async def handle_user_id(msg: int) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert list(schema["channels"].keys()) == [
            IsStr(regex=r"test[\w:]*:\[HandleUserCreated,HandleUserId\]"),
        ]

        assert list(schema["components"]["messages"].keys()) == [
            IsStr(
                regex=r"test[\w:]*:\[HandleUserCreated,HandleUserId\]:SubscribeMessage",
            ),
        ]

        assert list(schema["components"]["schemas"].keys()) == [
            "HandleUserCreated:Message:Payload",
            "HandleUserId:Message:Payload",
        ]

    def test_subscriber_filter_pydantic(self) -> None:
        broker = self.broker_class()

        sub = broker.subscriber("test")

        @sub
        async def handle_user_created(msg: create_model("SimpleModel")) -> None: ...

        @sub
        async def handle_user_id(msg: int) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert list(schema["channels"].keys()) == [
            IsStr(regex=r"test[\w:]*:\[HandleUserCreated,HandleUserId\]"),
        ]

        assert list(schema["components"]["messages"].keys()) == [
            IsStr(
                regex=r"test[\w:]*:\[HandleUserCreated,HandleUserId\]:SubscribeMessage",
            ),
        ]

        assert list(schema["components"]["schemas"].keys()) == [
            "SimpleModel",
            "HandleUserId:Message:Payload",
        ]

    def test_subscriber_filter_with_title(self) -> None:
        broker = self.broker_class()

        sub = broker.subscriber("test", title="custom")

        @sub
        async def handle_user_created(msg: str) -> None: ...

        @sub
        async def handle_user_id(msg: int) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert list(schema["channels"].keys()) == ["custom"]

        assert list(schema["components"]["messages"].keys()) == [
            "custom:SubscribeMessage",
        ]

        assert list(schema["components"]["schemas"].keys()) == [
            "HandleUserCreated:Message:Payload",
            "HandleUserId:Message:Payload",
        ]


class PublisherNaming(BaseNaming):
    def test_publisher_naming_base(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test")
        async def handle_user_created() -> str: ...

        schema = self.get_spec(broker).to_jsonable()

        assert list(schema["channels"].keys()) == [IsStr(regex=r"test[\w:]*:Publisher")]

        assert list(schema["components"]["messages"].keys()) == [
            IsStr(regex=r"test[\w:]*:Publisher:Message"),
        ]

        assert list(schema["components"]["schemas"].keys()) == [
            IsStr(regex=r"test[\w:]*:Publisher:Message:Payload"),
        ]

    def test_publisher_naming_pydantic(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test")
        async def handle_user_created() -> create_model("SimpleModel"): ...

        schema = self.get_spec(broker).to_jsonable()

        assert list(schema["channels"].keys()) == [IsStr(regex=r"test[\w:]*:Publisher")]

        assert list(schema["components"]["messages"].keys()) == [
            IsStr(regex=r"test[\w:]*:Publisher:Message"),
        ]

        assert list(schema["components"]["schemas"].keys()) == [
            "SimpleModel",
        ], list(schema["components"]["schemas"].keys())

    def test_publisher_manual_naming(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test", title="custom")
        async def handle_user_created() -> str: ...

        schema = self.get_spec(broker).to_jsonable()

        assert list(schema["channels"].keys()) == ["custom"]

        assert list(schema["components"]["messages"].keys()) == ["custom:Message"]

        assert list(schema["components"]["schemas"].keys()) == [
            "custom:Message:Payload",
        ]

    def test_publisher_with_schema_naming(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test", schema=str)
        async def handle_user_created() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert list(schema["channels"].keys()) == [IsStr(regex=r"test[\w:]*:Publisher")]

        assert list(schema["components"]["messages"].keys()) == [
            IsStr(regex=r"test[\w:]*:Publisher:Message"),
        ]

        assert list(schema["components"]["schemas"].keys()) == [
            IsStr(regex=r"test[\w:]*:Publisher:Message:Payload"),
        ]

    def test_publisher_manual_naming_with_schema(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test", title="custom", schema=str)
        async def handle_user_created() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert list(schema["channels"].keys()) == ["custom"]

        assert list(schema["components"]["messages"].keys()) == ["custom:Message"]

        assert list(schema["components"]["schemas"].keys()) == [
            "custom:Message:Payload",
        ]

    def test_multi_publishers_naming(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test")
        @broker.publisher("test2")
        async def handle_user_created() -> str: ...

        schema = self.get_spec(broker).to_jsonable()

        names = list(schema["channels"].keys())
        assert names == Contains(
            IsStr(regex=r"test2[\w:]*:Publisher"),
            IsStr(regex=r"test[\w:]*:Publisher"),
        ) & HasLen(2), names

        messages = list(schema["components"]["messages"].keys())
        assert messages == Contains(
            IsStr(regex=r"test2[\w:]*:Publisher:Message"),
            IsStr(regex=r"test[\w:]*:Publisher:Message"),
        ) & HasLen(2), messages

        payloads = list(schema["components"]["schemas"].keys())
        assert payloads == Contains(
            IsStr(regex=r"test2[\w:]*:Publisher:Message:Payload"),
            IsStr(regex=r"test[\w:]*:Publisher:Message:Payload"),
        ) & HasLen(2), payloads

    def test_multi_publisher_usages(self) -> None:
        broker = self.broker_class()

        pub = broker.publisher("test")

        @pub
        async def handle_user_created() -> str: ...

        @pub
        async def handle() -> int: ...

        schema = self.get_spec(broker).to_jsonable()

        assert list(schema["channels"].keys()) == [
            IsStr(regex=r"test[\w:]*:Publisher"),
        ]

        assert list(schema["components"]["messages"].keys()) == [
            IsStr(regex=r"test[\w:]*:Publisher:Message"),
        ]

        assert list(schema["components"]["schemas"].keys()) == [
            "HandleUserCreated:Publisher:Message:Payload",
            "Handle:Publisher:Message:Payload",
        ], list(schema["components"]["schemas"].keys())

    def test_multi_publisher_usages_with_custom(self) -> None:
        broker = self.broker_class()

        pub = broker.publisher("test", title="custom")

        @pub
        async def handle_user_created() -> str: ...

        @pub
        async def handle() -> int: ...

        schema = self.get_spec(broker).to_jsonable()

        assert list(schema["channels"].keys()) == ["custom"]

        assert list(schema["components"]["messages"].keys()) == ["custom:Message"]

        assert list(schema["components"]["schemas"].keys()) == [
            "HandleUserCreated:Publisher:Message:Payload",
            "Handle:Publisher:Message:Payload",
        ]


class NamingTestCase(MultibrokersSchema, SubscriberNaming, FilterNaming, PublisherNaming):
    pass
