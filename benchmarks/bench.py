import asyncio
import csv
import json
import platform
import resource
import statistics
import sys
import time
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import psutil
from kafka_cases import KAFKA_CASES

from faststream.__about__ import __version__

BENCHMARKS = {
    "kafka": KAFKA_CASES
}

PREFILL_SIZES = (200_000, 500_000, 1_000_000)

REPEATS = 1

RESULTS_DIR = Path(__file__).resolve().parent
CSV_REPORT = RESULTS_DIR / "new_benches.csv"
JSON_REPORT = RESULTS_DIR / "new_benches.jsonl"

FIELDNAMES = (
    "faststream_version",
    "scenario",
    "transport",
    "implementation",
    "broker",
    "comment",
    "N",
    "run_idx",
    "prefetch",
    "batch",
    "ack_mode",
    "events_per_second",
    "elapsed_time_s",
    "cpu_ms_per_1k",
    "peak_rss_mb",
    "rss_mb_median",
    "rtt_p50_ms",
    "rtt_p95_ms",
    "rtt_p99_ms",
    "dropped",
    "duplicated",
    "measured_at",
    "python_version",
    "host_memory_gb",
)


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
    cpu_ms_per_1k: float
    rss_mb_median: float
    rtt_samples_ms: list[float] = field(default_factory=list)
    dropped: int | None = None
    duplicated: int | None = None

    @property
    def eps(self) -> float:
        return self.total_events / self.elapsed_time


def _percentile(samples: Sequence[float], percent: float) -> float | None:
    """Linear-interpolated percentile of an unsorted sample sequence."""
    if not samples:
        return None
    ordered = sorted(samples)
    if len(ordered) == 1:
        return ordered[0]
    rank = percent / 100 * (len(ordered) - 1)
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)


def _peak_rss_mb() -> float:
    max_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return max_rss / (1024**2) if sys.platform == "darwin" else max_rss / 1024


async def measure(
    case: TestCase,
    target: int,
) -> AsyncGenerator[MeasureResult, None]:
    process = psutil.Process()
    cpu_start = process.cpu_times()
    rss_samples_mb: list[float] = []

    async with case.start() as start_time:
        while target > case.EVENTS_PROCESSED:
            print(f"Obrabotano {case.EVENTS_PROCESSED} / {target}")
            rss_samples_mb.append(process.memory_info().rss / (1024**2))
            await asyncio.sleep(1.0)

    cpu_end = process.cpu_times()
    elapsed_time = time.time() - start_time

    cpu_delta_ms = (
        (cpu_end.user + cpu_end.system) - (cpu_start.user + cpu_start.system)
    ) * 1000
    cpu_ms_per_1k = cpu_delta_ms / (target / 1000) if target else 0.0

    rss_mb_median = (
        statistics.median(rss_samples_mb)
        if rss_samples_mb
        else process.memory_info().rss / (1024**2)
    )

    yield MeasureResult(
        total_events=case.EVENTS_PROCESSED,
        elapsed_time=elapsed_time,
        cpu_ms_per_1k=cpu_ms_per_1k,
        rss_mb_median=rss_mb_median,
        rtt_samples_ms=[float(v) for v in getattr(case, "rtt_samples_ms", [])],
        dropped=getattr(case, "dropped", None),
        duplicated=getattr(case, "duplicated", None),
    )


async def main(case: TestCase, prefill_messages: int) -> MeasureResult:
    await case.setup_method(prefill_messages=prefill_messages)
    async for result in measure(case, prefill_messages):
        sys.stdout.write(
            f"\rpassed time: {result.elapsed_time:.1f}) EPS: {result.eps:.2f}"
        )

    return result


def _implementation_of(case_cls: type) -> str:
    name = case_cls.__name__
    if "Faststream" in name:
        return "Faststream"
    if "Pure" in name:
        return "Pure"
    return "Unknown"



def _append_csv(path: Path, fieldnames: Sequence[str], row: dict[str, Any]) -> None:
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=";")
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def _append_json(path: Path, row: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as jsonfile:
        jsonfile.write(json.dumps(row) + "\n")


if __name__ == "__main__":
    for transport, test_cases in BENCHMARKS.items():
        for scenario, case_classes in test_cases.items():
            for case_cls in case_classes:
                for prefill_messages in PREFILL_SIZES:
                    for run_idx in range(REPEATS):
                        case = case_cls()
                        final_result = asyncio.run(main(case, prefill_messages))

                        print(
                            f"\nEvents per second: {final_result.eps:.2f} "
                            f"(scenario={scenario}, transport={transport}, "
                            f"N={prefill_messages}, run_idx={run_idx})"
                        )

                        mem = psutil.virtual_memory()

                        row: dict[str, Any] = {
                            "faststream_version": __version__,
                            "scenario": scenario,
                            "transport": transport,
                            "implementation": _implementation_of(case_cls),
                            "broker": case.broker_type,
                            "comment": case.comment,
                            "N": prefill_messages,
                            "run_idx": run_idx,
                            "prefetch": getattr(case, "prefetch", None),
                            "batch": getattr(case, "batch", None),
                            "ack_mode": getattr(case, "ack_mode", None),
                            "events_per_second": round(final_result.eps, 2),
                            "elapsed_time_s": round(final_result.elapsed_time, 3),
                            "cpu_ms_per_1k": round(final_result.cpu_ms_per_1k, 3),
                            "peak_rss_mb": round(_peak_rss_mb(), 2),
                            "rss_mb_median": round(final_result.rss_mb_median, 2),
                            "rtt_p50_ms": _percentile(final_result.rtt_samples_ms, 50),
                            "rtt_p95_ms": _percentile(final_result.rtt_samples_ms, 95),
                            "rtt_p99_ms": _percentile(final_result.rtt_samples_ms, 99),
                            "dropped": final_result.dropped,
                            "duplicated": final_result.duplicated,
                            "measured_at": datetime.now(tz=timezone.utc).isoformat(),
                            "python_version": platform.python_version(),
                            "host_memory_gb": round(mem.total / (1024**3), 2),
                        }

                        _append_csv(CSV_REPORT, FIELDNAMES, row)
                        _append_json(JSON_REPORT, row)
