"""In-memory observability metrics recorder.

Phase 5 で必要な latency / error rate / job duration / J-Quants cache hit を
軽量に採取するための集約器。
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from threading import Lock


@dataclass
class DurationMetric:
    count: int = 0
    total_ms: float = 0.0

    def observe(self, value_ms: float) -> None:
        self.count += 1
        self.total_ms += value_ms


class MetricsRecorder:
    """Process-local metrics recorder."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._request_total = 0
        self._request_errors = 0
        self._request_latency: dict[tuple[str, str], DurationMetric] = defaultdict(DurationMetric)
        self._job_duration: dict[tuple[str, str], DurationMetric] = defaultdict(DurationMetric)
        self._jquants_fetch_total: dict[str, int] = defaultdict(int)
        self._jquants_cache_state_total: dict[tuple[str, str], int] = defaultdict(int)

    def record_request(self, method: str, path: str, status: int, elapsed_ms: float) -> None:
        key = (method, path)
        with self._lock:
            self._request_total += 1
            if status >= 400:
                self._request_errors += 1
            self._request_latency[key].observe(elapsed_ms)

    def record_job_duration(self, job_type: str, status: str, elapsed_ms: float) -> None:
        with self._lock:
            self._job_duration[(job_type, status)].observe(elapsed_ms)

    def record_jquants_fetch(self, endpoint: str) -> None:
        with self._lock:
            self._jquants_fetch_total[endpoint] += 1

    def record_jquants_cache_state(self, endpoint: str, state: str) -> None:
        with self._lock:
            self._jquants_cache_state_total[(endpoint, state)] += 1

    def error_rate(self) -> float:
        with self._lock:
            if self._request_total == 0:
                return 0.0
            return self._request_errors / self._request_total


metrics_recorder = MetricsRecorder()

