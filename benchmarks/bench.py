import asyncio
import csv
import platform
import sys
import time
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

import psutil

from faststream.__about__ import __version__


class TestCase(Protocol):
    EVENTS_PROCESSED: int
    broker_type: str
    comment: str

    @asynccontextmanager
    async def start(self) -> AsyncIterator[float]: ...


@dataclass
class MeasureResult:
    total_events: int
    elapsed_time: float

    @property
    def eps(self) -> float:
        return self.total_events / self.elapsed_time


async def measure(
    case: TestCase, measure_time: int
) -> AsyncGenerator[MeasureResult, None]:
    async with case.start() as start_time:
        while (elapsed_time := (time.time() - start_time)) < measure_time:
            yield MeasureResult(case.EVENTS_PROCESSED, elapsed_time)
            await asyncio.sleep(1.0)

    yield MeasureResult(case.EVENTS_PROCESSED, time.time() - start_time)


async def main(case: TestCase, measure_time: int) -> MeasureResult:
    async for result in measure(case, measure_time):
        sys.stdout.write(
            f"\rTotal events: {result.total_events}, elapsed time: "
            f"{result.elapsed_time:.1f}s ({(measure_time - result.elapsed_time):.1f} left), "
            f"EPS: {result.eps:.2f}"
        )

    return result


if __name__ == "__main__":
    from rabbit_cases.basic import RabbitTestCase

    case: TestCase = RabbitTestCase()

    bench_file = Path(__file__).resolve().parent / "benches.csv"

    final_result = asyncio.run(main(case, 60 * 10))

    print(f"\nTotal events: {final_result.total_events}")
    print(f"Events per second: {(final_result.eps):.2f}")

    with bench_file.open("a", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")

        if csvfile.tell() == 0:
            writer.writerow([
                "FastStream Version",
                "Broker",
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
            final_result.total_events,
            round(final_result.eps, 2),
            final_result.elapsed_time,
            datetime.now(tz=timezone.utc).isoformat(),
            platform.python_version(),
            case.comment,
            f"{mem.total / (1024**3):.2f} GB",
        ])
