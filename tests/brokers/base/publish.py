import asyncio
from abc import abstractmethod
from datetime import datetime
from typing import Any, ClassVar, Dict, List, Tuple
from unittest.mock import Mock

import anyio
import pytest
from pydantic import BaseModel

from faststream._compat import model_to_json
from faststream.annotations import Logger
from faststream.broker.core.usecase import BrokerUsecase


class SimpleModel(BaseModel):
    r: str


now = datetime.now()


class BrokerPublishTestcase:
    timeout: int = 3
    subscriber_kwargs: ClassVar[Dict[str, Any]] = {}

    @abstractmethod
    def get_broker(self, apply_types: bool = False) -> BrokerUsecase[Any, Any]:
        raise NotImplementedError

    def patch_broker(self, broker: BrokerUsecase[Any, Any]) -> BrokerUsecase[Any, Any]:
        return broker

    @pytest.mark.asyncio()
    @pytest.mark.parametrize(
        ("message", "message_type", "expected_message"),
        (  # noqa: PT007
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
                False,
                bool,
                False,
                id="bool->bool",
            ),
            pytest.param(
                {"m": 1},
                Dict[str, int],
                {"m": 1},
                id="dict->dict",
            ),
            pytest.param(
                [1, 2, 3],
                List[int],
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
        ),
    )
    async def test_serialize(
        self,
        mock: Mock,
        queue: str,
        message,
        message_type,
        expected_message,
        event,
    ):
        pub_broker = self.get_broker(apply_types=True)

        @pub_broker.subscriber(queue, **self.subscriber_kwargs)
        async def handler(m: message_type, logger: Logger):
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
    async def test_unwrap_dict(
        self,
        mock: Mock,
        queue: str,
        event,
    ):
        pub_broker = self.get_broker(apply_types=True)

        @pub_broker.subscriber(queue, **self.subscriber_kwargs)
        async def m(a: int, b: int, logger: Logger):
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
            }
        )

    @pytest.mark.asyncio()
    async def test_unwrap_list(
        self,
        mock: Mock,
        queue: str,
        event: asyncio.Event,
    ):
        pub_broker = self.get_broker(apply_types=True)

        @pub_broker.subscriber(queue, **self.subscriber_kwargs)
        async def m(a: int, b: int, *args: Tuple[int, ...], logger: Logger):
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
        event,
        mock,
    ):
        pub_broker = self.get_broker(apply_types=True)

        @pub_broker.subscriber(queue, **self.subscriber_kwargs)
        @pub_broker.publisher(queue + "resp")
        async def m():
            return ""

        @pub_broker.subscriber(queue + "resp", **self.subscriber_kwargs)
        async def resp(msg):
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
        event,
        mock,
    ):
        pub_broker = self.get_broker(apply_types=True)

        publisher = pub_broker.publisher(queue + "resp")

        @publisher
        @pub_broker.subscriber(queue, **self.subscriber_kwargs)
        async def m():
            return ""

        @pub_broker.subscriber(queue + "resp", **self.subscriber_kwargs)
        async def resp(msg):
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
        event,
        mock,
    ):
        pub_broker = self.get_broker(apply_types=True)

        publisher = pub_broker.publisher(queue + "resp")

        @pub_broker.subscriber(queue, **self.subscriber_kwargs)
        async def m():
            await publisher.publish("")

        @pub_broker.subscriber(queue + "resp", **self.subscriber_kwargs)
        async def resp(msg):
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
        mock,
    ):
        pub_broker = self.get_broker(apply_types=True)

        event = anyio.Event()
        event2 = anyio.Event()

        @pub_broker.publisher(queue + "resp2")
        @pub_broker.subscriber(queue, **self.subscriber_kwargs)
        @pub_broker.publisher(queue + "resp")
        async def m():
            return ""

        @pub_broker.subscriber(queue + "resp", **self.subscriber_kwargs)
        async def resp(msg):
            event.set()
            mock.resp1(msg)

        @pub_broker.subscriber(queue + "resp2", **self.subscriber_kwargs)
        async def resp2(msg):
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
        mock,
    ):
        pub_broker = self.get_broker(apply_types=True)

        consume = anyio.Event()
        consume2 = anyio.Event()

        pub = pub_broker.publisher(queue + "resp")

        @pub
        @pub_broker.subscriber(queue, **self.subscriber_kwargs)
        async def m():
            return ""

        @pub
        @pub_broker.subscriber(queue + "2", **self.subscriber_kwargs)
        async def m2():
            return ""

        @pub_broker.subscriber(queue + "resp", **self.subscriber_kwargs)
        async def resp():
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
        event,
        mock,
    ):
        pub_broker = self.get_broker(apply_types=True)

        @pub_broker.subscriber(queue + "reply", **self.subscriber_kwargs)
        async def reply_handler(m):
            event.set()
            mock(m)

        @pub_broker.subscriber(queue, **self.subscriber_kwargs)
        async def handler(m):
            return m

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(
                        br.publish("Hello!", queue, reply_to=queue + "reply")
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with("Hello!")

    @pytest.mark.asyncio()
    async def test_publisher_after_start(
        self,
        queue: str,
        event,
        mock,
    ):
        pub_broker = self.get_broker(apply_types=True)

        @pub_broker.subscriber(queue, **self.subscriber_kwargs)
        async def handler(m):
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
