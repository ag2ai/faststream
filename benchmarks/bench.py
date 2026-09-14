import asyncio
import csv
import platform
import sys
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

import psutil
from confluent_cases import CONFLUENT_CASES
from kafka_cases import KAFKA_CASES
from nats_cases import NATS_CASES
from rabbit_cases import RABBIT_CASES
from redis_cases import REDIS_CASES

from faststream.__about__ import __version__

BENCHMARKS = {
    "kafka": KAFKA_CASES,
    "nats": NATS_CASES,
    "rabbit": RABBIT_CASES,
    "redis": REDIS_CASES,
    "confluent": CONFLUENT_CASES,
}

PREFILL_SIZES = (200_000, 500_000, 1_000_000)


class TestCase(Protocol):
    EVENTS_PROCESSED: int
    broker_type: str
    comment: str

    async def setup_method(self, **kwargs: object) -> None: ...

    @asynccontextmanager
    def start(self) -> AsyncGenerator[float, None]: ...

    def test_consume_message(self) -> AsyncGenerator[None, None]: ...


@dataclass
class MeasureResult:
    total_events: int
    elapsed_time: float

    @property
    def eps(self) -> float:
        return self.total_events / self.elapsed_time


async def measure(
    case: TestCase,
    target: int,
) -> AsyncGenerator[MeasureResult, None]:
    async with case.start() as start_time:
        while case.EVENTS_PROCESSED < target:
            print(f"Obrabotano {case.EVENTS_PROCESSED} / {target}")
            await asyncio.sleep(1.0)
    yield MeasureResult(case.EVENTS_PROCESSED, time.time() - start_time)


async def main(case: TestCase, prefill_messages: int) -> MeasureResult:
    await case.setup_method(prefill_messages=prefill_messages)
    async for result in measure(case, prefill_messages):
        sys.stdout.write(
            f"\rTotal events: {result.total_events}, passed time: "
            f"{result.elapsed_time:.1f}) "
            f"EPS: {result.eps:.2f}"
        )

    return result


if __name__ == "__main__":
    for broker, test_cases in BENCHMARKS.items():
        test_case_classes = [
            case_cls for cases in test_cases.values() for case_cls in cases
        ]
        for case_cls in test_case_classes:
            for prefill_messages in PREFILL_SIZES:
                case = case_cls()
                bench_file = Path(__file__).resolve().parent / "new_benches.csv"
                final_result = asyncio.run(main(case, prefill_messages))

                print(f"\nTotal events: {final_result.total_events}")
                print(f"Events per second: {(final_result.eps):.2f}")

                with bench_file.open("a", newline="") as csvfile:
                    writer = csv.writer(csvfile, delimiter=";")

                    if csvfile.tell() == 0:
                        writer.writerow([
                            "FastStream Version",
                            "Broker",
                            "Prefill Messages",
                            "Total Events",
                            "Event per second",
                            "Elapsed Time",
                            "Measure Time",
                            "Python Version",
                            "Comments",
                            "Host Memory",
                        ])

                    mem = psutil.virtual_memory()

                    writer.writerow([
                        __version__,
                        case.broker_type,
                        prefill_messages,
                        final_result.total_events,
                        round(final_result.eps, 2),
                        final_result.elapsed_time,
                        datetime.now(tz=timezone.utc).isoformat(),
                        platform.python_version(),
                        case.comment,
                        f"{mem.total / (1024**3):.2f} GB",
                    ])
