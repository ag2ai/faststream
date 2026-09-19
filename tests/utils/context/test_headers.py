import pytest

from faststream import Header
from tests.marks import require_nats


@pytest.mark.asyncio()
@require_nats
async def test_nats_headers() -> None:
    from faststream.nats import NatsBroker, TestNatsBroker

    broker = NatsBroker()

    @broker.subscriber("in")
    async def h(
        name: str = Header(),
        id_: int = Header("id"),
    ) -> dict[str, str | int]:
        return {"name": name, "id": id_}

    async with TestNatsBroker(broker) as br:
        assert await (
            await br.request(
                "",
                "in",
                headers={
                    "name": "john",
                    "id": "1",
                },
                timeout=1.0,
            )
        ).decode() == {"name": "john", "id": 1}
