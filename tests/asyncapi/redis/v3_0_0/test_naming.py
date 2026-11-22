from typing import Any

import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.redis import RedisBroker
from tests.asyncapi.base.v3_0_0.naming import NamingTestCase


@pytest.mark.redis()
class TestNaming(NamingTestCase):
    broker_class = RedisBroker

    def test_base(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json

    @pytest.mark.parametrize(
        "args",
        (
            pytest.param({"channel": "test"}, id="channel"),
            pytest.param({"list": "test"}, id="list"),
            pytest.param({"stream": "test"}, id="stream"),
        ),
    )
    def test_subscribers_variations(
        self,
        args: dict[str, Any],
        snapshot_json: SnapshotAssertion,
    ) -> None:
        broker = self.broker_class()

        @broker.subscriber(**args)  # type: ignore[untyped-decorator]
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json

    @pytest.mark.parametrize(
        "args",
        (
            pytest.param({"channel": "test"}, id="channel"),
            pytest.param({"list": "test"}, id="list"),
            pytest.param({"stream": "test"}, id="stream"),
        ),
    )
    def test_publisher_variations(
        self,
        args: dict[str, Any],
        snapshot_json: SnapshotAssertion,
    ) -> None:
        broker = self.broker_class()

        @broker.publisher(**args)  # type: ignore[untyped-decorator]
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json
