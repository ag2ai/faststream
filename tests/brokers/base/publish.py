import asyncio
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any
from unittest.mock import Mock

import anyio
import pytest
from pydantic import BaseModel

from faststream import BaseMiddleware, Context, Response
from faststream._internal._compat import dump_json, model_to_json

from .basic import BaseTestcaseConfig


class SimpleModel(BaseModel):
    r: str


@dataclass
class SimpleDataclass:
    r: str


now = datetime.now(timezone.utc)


class BrokerPublishTestcase(BaseTestcaseConfig):
    @pytest.mark.asyncio()
    @pytest.mark.parametrize(
        ("message", "message_type", "expected_message"),
        (
            pytest.param(
                "hello",
                str,
                "hello",
                id="str->str",
            ),
            pytest.param(
                b"hello",
                bytes,
                b"hello",
                id="bytes->bytes",
            ),
            pytest.param(
                1,
                int,
                1,
                id="int->int",
            ),
            pytest.param(
                1.0,
                float,
                1.0,
                id="float->float",
            ),
            pytest.param(
                1,
                float,
                1.0,
                id="int->float",
            ),
            pytest.param(
                False,
                bool,
                False,
                id="bool->bool",
            ),
            pytest.param(
                {"m": 1},
                dict[str, int],
                {"m": 1},
                id="dict->dict",
            ),
            pytest.param(
                [1, 2, 3],
                list[int],
                [1, 2, 3],
                id="list->list",
            ),
            pytest.param(
                now,
                datetime,
                now,
                id="datetime->datetime",
            ),
            pytest.param(
                model_to_json(SimpleModel(r="hello!")).encode(),
                SimpleModel,
                SimpleModel(r="hello!"),
                id="bytes->model",
            ),
            pytest.param(
                SimpleModel(r="hello!"),
                SimpleModel,
                SimpleModel(r="hello!"),
                id="model->model",
            ),
            pytest.param(
                SimpleModel(r="hello!"),
                dict,
                {"r": "hello!"},
                id="model->dict",
            ),
            pytest.param(
                {"r": "hello!"},
                SimpleModel,
                SimpleModel(r="hello!"),
                id="dict->model",
            ),
            pytest.param(
                dump_json(asdict(SimpleDataclass(r="hello!"))),
                SimpleDataclass,
                SimpleDataclass(r="hello!"),
                id="bytes->dataclass",
            ),
            pytest.param(
                SimpleDataclass(r="hello!"),
                SimpleDataclass,
                SimpleDataclass(r="hello!"),
                id="dataclass->dataclass",
            ),
            pytest.param(
                SimpleDataclass(r="hello!"),
                dict,
                {"r": "hello!"},
                id="dataclass->dict",
            ),
            pytest.param(
                {"r": "hello!"},
                SimpleDataclass,
                SimpleDataclass(r="hello!"),
                id="dict->dataclass",
            ),
        ),
    )
    async def test_serialize(
        self,
        queue: str,
        message,
        message_type,
        expected_message,
        mock: Mock,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
        async def handler(m: message_type) -> None:
            event.set()
            mock(m)

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish(message, queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with(expected_message)

    @pytest.mark.asyncio()
    async def test_response(
        self,
        queue: str,
        mock: Mock,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
        @pub_broker.publisher(queue + "1")
        async def m():
            return Response(1, headers={"custom": "1"}, correlation_id="1")

        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        @pub_broker.subscriber(*args2, **kwargs2)
        async def m_next(msg=Context("message")) -> None:
            event.set()
            mock(
                body=msg.body,
                headers=msg.headers["custom"],
                correlation_id=msg.correlation_id,
            )

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish(None, queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with(
            body=b"1",
            correlation_id="1",
            headers="1",
        )

    @pytest.mark.asyncio()
    async def test_unwrap_dict(
        self,
        queue: str,
        mock: Mock,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
        async def m(a: int, b: int) -> None:
            event.set()
            mock({"a": a, "b": b})

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish({"a": 1, "b": 1.0}, queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with(
            {
                "a": 1,
                "b": 1,
            },
        )

    @pytest.mark.asyncio()
    async def test_unwrap_list(
        self,
        mock: Mock,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
        async def m(a: int, b: int, *args: tuple[int, ...]) -> None:
            event.set()
            mock({"a": a, "b": b, "args": args})

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish([1, 1.0, 2.0, 3.0], queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with({"a": 1, "b": 1, "args": (2, 3)})

    @pytest.mark.asyncio()
    async def test_base_publisher(
        self,
        queue: str,
        mock: Mock,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
        @pub_broker.publisher(queue + "resp")
        async def m() -> str:
            return ""

        args2, kwargs2 = self.get_subscriber_params(queue + "resp")

        @pub_broker.subscriber(*args2, **kwargs2)
        async def resp(msg) -> None:
            event.set()
            mock(msg)

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_once_with("")

    @pytest.mark.asyncio()
    async def test_publisher_object(
        self,
        queue: str,
        mock: Mock,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker(apply_types=True)

        publisher = pub_broker.publisher(queue + "resp")

        args, kwargs = self.get_subscriber_params(queue)

        @publisher
        @pub_broker.subscriber(*args, **kwargs)
        async def m() -> str:
            return ""

        args, kwargs = self.get_subscriber_params(queue + "resp")

        @pub_broker.subscriber(*args, **kwargs)
        async def resp(msg) -> None:
            event.set()
            mock(msg)

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_once_with("")

    @pytest.mark.asyncio()
    async def test_publish_manual(
        self,
        queue: str,
        mock: Mock,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker(apply_types=True)

        publisher = pub_broker.publisher(queue + "resp")

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
        async def m() -> None:
            await publisher.publish("")

        args2, kwargs2 = self.get_subscriber_params(queue + "resp")

        @pub_broker.subscriber(*args2, **kwargs2)
        async def resp(msg) -> None:
            event.set()
            mock(msg)

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_once_with("")

    @pytest.mark.asyncio()
    async def test_multiple_publishers(
        self,
        queue: str,
        mock: Mock,
    ) -> None:
        pub_broker = self.get_broker(apply_types=True)

        event = anyio.Event()
        event2 = anyio.Event()

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.publisher(queue + "resp2")
        @pub_broker.subscriber(*args, **kwargs)
        @pub_broker.publisher(queue + "resp")
        async def m() -> str:
            return ""

        args2, kwargs2 = self.get_subscriber_params(queue + "resp")

        @pub_broker.subscriber(*args2, **kwargs2)
        async def resp(msg) -> None:
            event.set()
            mock.resp1(msg)

        args3, kwargs3 = self.get_subscriber_params(queue + "resp2")

        @pub_broker.subscriber(*args3, **kwargs3)
        async def resp2(msg) -> None:
            event2.set()
            mock.resp2(msg)

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(event.wait()),
                    asyncio.create_task(event2.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert event2.is_set()
        mock.resp1.assert_called_once_with("")
        mock.resp2.assert_called_once_with("")

    @pytest.mark.asyncio()
    async def test_reusable_publishers(
        self,
        queue: str,
        mock: Mock,
    ) -> None:
        pub_broker = self.get_broker(apply_types=True)

        consume = anyio.Event()
        consume2 = anyio.Event()

        pub = pub_broker.publisher(queue + "resp")

        args, kwargs = self.get_subscriber_params(queue)

        @pub
        @pub_broker.subscriber(*args, **kwargs)
        async def m() -> str:
            return ""

        args2, kwargs2 = self.get_subscriber_params(queue + "2")

        @pub
        @pub_broker.subscriber(*args2, **kwargs2)
        async def m2() -> str:
            return ""

        args3, kwargs3 = self.get_subscriber_params(queue + "resp")

        @pub_broker.subscriber(*args3, **kwargs3)
        async def resp() -> None:
            if not consume.is_set():
                consume.set()
            else:
                consume2.set()
            mock()

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(br.publish("", queue + "2")),
                    asyncio.create_task(consume.wait()),
                    asyncio.create_task(consume2.wait()),
                ),
                timeout=self.timeout,
            )

        assert consume2.is_set()
        assert consume.is_set()
        assert mock.call_count == 2

    @pytest.mark.asyncio()
    async def test_reply_to(
        self,
        queue: str,
        mock: Mock,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(queue + "reply")

        @pub_broker.subscriber(*args, **kwargs)
        async def reply_handler(m) -> None:
            event.set()
            mock(m)

        args2, kwargs2 = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args2, **kwargs2)
        async def handler(m):
            return m

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(
                        br.publish("Hello!", queue, reply_to=queue + "reply"),
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with("Hello!")

    @pytest.mark.asyncio()
    async def test_no_reply(
        self,
        queue: str,
        mock: Mock,
    ) -> None:
        event = asyncio.Event()

        class Mid(BaseMiddleware):
            async def after_processed(self, *args: Any, **kwargs: Any):
                event.set()

                return await super().after_processed(*args, **kwargs)

        pub_broker = self.get_broker(apply_types=True)
        pub_broker.add_middleware(Mid)

        args, kwargs = self.get_subscriber_params(queue + "reply")

        @pub_broker.subscriber(*args, **kwargs)
        async def reply_handler(m) -> None:
            mock(m)

        args2, kwargs2 = self.get_subscriber_params(queue, no_reply=True)

        @pub_broker.subscriber(*args2, **kwargs2)
        async def handler(m):
            return m

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(
                        br.publish("Hello!", queue, reply_to=queue + "reply"),
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert not mock.called

    @pytest.mark.asyncio()
    async def test_publisher_after_connect(self, queue: str) -> None:
        async with self.patch_broker(self.get_broker()) as br:
            # Should pass without error
            await br.publisher(queue).publish(None)

    @pytest.mark.asyncio()
    async def test_publisher_after_start(
        self,
        queue: str,
        mock: Mock,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
        async def handler(m) -> None:
            event.set()
            mock(m)

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            pub = br.publisher(queue)

            await asyncio.wait(
                (
                    asyncio.create_task(pub.publish("Hello!")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with("Hello!")
