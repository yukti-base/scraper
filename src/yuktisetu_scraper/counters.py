"""Per-platform request counters.

Kept in memory and served on /health. A count of every request by outcome is how
"CodeChef started refusing us at 18:32" becomes a fact someone can read rather
than an inference from a pile of failed jobs, and the latency figures are the
early warning: a platform that is about to block usually slows down first.
"""

from __future__ import annotations

import time
from collections import Counter, deque
from dataclasses import dataclass, field

from .models import SnapshotStatus

_WINDOW_SECONDS = 3600
_BUCKET_SECONDS = 60


@dataclass
class _Bucket:
    minute: int
    outcomes: Counter = field(default_factory=Counter)
    latency_ms_total: float = 0.0
    requests: int = 0


class RequestCounters:
    """Outcome counts over the last minute and the last hour, plus since start."""

    def __init__(self) -> None:
        self._buckets: deque[_Bucket] = deque()
        self._since_start: Counter = Counter()

    def record(self, status: SnapshotStatus, latency_ms: float) -> None:
        bucket = self._bucket_for(int(time.time()) // _BUCKET_SECONDS)
        bucket.outcomes[status.value] += 1
        bucket.requests += 1
        bucket.latency_ms_total += latency_ms
        self._since_start[status.value] += 1

    def snapshot(self) -> dict:
        now_minute = int(time.time()) // _BUCKET_SECONDS
        self._evict(now_minute)
        last_minute = Counter()
        last_hour = Counter()
        hour_requests, hour_latency = 0, 0.0
        for b in self._buckets:
            last_hour.update(b.outcomes)
            hour_requests += b.requests
            hour_latency += b.latency_ms_total
            if b.minute == now_minute:
                last_minute.update(b.outcomes)
        return {
            "last_minute": dict(last_minute),
            "last_hour": dict(last_hour),
            "since_start": dict(self._since_start),
            "avg_latency_ms_last_hour": round(hour_latency / hour_requests) if hour_requests else None,
        }

    def _bucket_for(self, minute: int) -> _Bucket:
        self._evict(minute)
        if not self._buckets or self._buckets[-1].minute != minute:
            self._buckets.append(_Bucket(minute))
        return self._buckets[-1]

    def _evict(self, now_minute: int) -> None:
        oldest = now_minute - _WINDOW_SECONDS // _BUCKET_SECONDS
        while self._buckets and self._buckets[0].minute <= oldest:
            self._buckets.popleft()
