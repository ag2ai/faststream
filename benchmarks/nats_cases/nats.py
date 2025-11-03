import json
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import nats

from .schemas.pydantic import Schema


class NatsTestCase:
    comment = "Pure nats_py client with pydantic"
    broker_type = "NATS"

    def __init__(self) -> None:
        self.EVENTS_PROCESSED = 0

    @asynccontextmanager
    async def start(self) -> AsyncIterator[float]:
        nc = await nats.connect(servers=["nats://localhost:4222"])

        async def message_handler(msg: Any) -> None:
            self.EVENTS_PROCESSED += 1
            data = json.loads(msg.data.decode("utf-8"))
            parsed = Schema(**data)

            await nc.publish(
                "in",
                parsed.model_dump_json().encode()
            )

        await nc.subscribe("in", cb=message_handler)

        start_time = time.time()

        await nc.publish(
            "in",
            json.dumps({
                "name": "John",
                "age": 39,
                "fullname": "LongString" * 8,
                "children": [{"name": "Mike", "age": 8, "fullname": "LongString" * 8}],
            }).encode("utf-8")
        )

        yield start_time

        await nc.close()
