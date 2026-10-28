"""The two decisions that bound how long anything can wait: retries and pauses.

Both are pure -- no clock, no database -- so the rules that stop a drive stalling
are tested directly rather than by waiting an hour in a test.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

from .models import SnapshotStatus


# ------------------------------------------------------------------ retries


@dataclass(frozen=True)
class RetryDecision:
    retry_at: datetime | None
    """When to try again, or None when the job is over."""
    reason: str


def after_failure(
    *,
    status: SnapshotStatus,
    attempts_done: int,
    created_at: datetime,
    give_up_at: datetime,
    offsets_minutes: list[int],
    now: datetime,
) -> RetryDecision:
    """What happens to a job whose attempt just failed.

    Only a platform's problem is retried. A handle that does not exist will not
    start existing, and a parser that broke needs a developer -- retrying either
    only burns the platform's patience. Those go straight to TnP review.

    Retry times are measured from when the job was QUEUED, not from the failure,
    so a slow queue cannot quietly push the last attempt past the hour. And
    give_up_at is absolute: if the next attempt would land after it, the job ends
    now instead, which is what makes "the drive waits for every job" a wait with a
    known end.
    """
    if not status.is_retryable:
        return RetryDecision(None, f"{status.value} is not worth retrying")

    if attempts_done > len(offsets_minutes):
        return RetryDecision(None, f"gave up after {attempts_done} attempts")

    retry_at = max(created_at + timedelta(minutes=offsets_minutes[attempts_done - 1]), now)
    if retry_at > give_up_at:
        return RetryDecision(
            None, f"next attempt at {retry_at.isoformat()} would pass the job's limit"
        )
    return RetryDecision(retry_at, f"attempt {attempts_done + 1} scheduled")


# ------------------------------------------------------------------ breaker


class BreakerState(str, Enum):
    CLOSED = "CLOSED"
    """Normal. Requests flow at the platform's rate."""
    OPEN = "OPEN"
    """Paused. Nothing is sent until paused_until."""
    HALF_OPEN = "HALF_OPEN"
    """The pause ended. Exactly one request goes out as a probe."""


@dataclass
class Breaker:
    """One platform's circuit breaker.

    The point is to stop at the FIRST sign of a block. Hammering a platform that
    has started refusing us is what turns a ten-minute block into a permanent
    one -- and a block lands on everyone behind the same IP.

    While OPEN, the lane claims nothing, so queued jobs do not spend attempts
    against a platform that has already said no. Their give_up_at still runs, so
    a long pause ends in TnP review rather than an endless wait.
    """

    pauses_minutes: list[int]
    error_threshold: int
    state: BreakerState = BreakerState.CLOSED
    paused_until: datetime | None = None
    consecutive_trips: int = 0
    consecutive_errors: int = 0
    last_reason: str | None = None

    def allows_request(self, now: datetime) -> bool:
        if self.state is BreakerState.OPEN:
            if self.paused_until is not None and now >= self.paused_until:
                self.state = BreakerState.HALF_OPEN
                return True
            return False
        return True

    def record(self, status: SnapshotStatus, now: datetime) -> bool:
        """Feed one outcome in. Returns True when the state changed."""
        if status is SnapshotStatus.RATE_LIMITED:
            return self._trip(now, "blocked or rate limited by the platform")

        if status is SnapshotStatus.PLATFORM_ERROR:
            self.consecutive_errors += 1
            if self.consecutive_errors >= self.error_threshold:
                return self._trip(
                    now, f"{self.consecutive_errors} consecutive platform errors"
                )
            if self.state is BreakerState.HALF_OPEN:
                # The probe failed: the platform is not back. Pause again.
                return self._trip(now, "probe failed with a platform error")
            return False

        # OK and HANDLE_NOT_FOUND both prove the platform is answering properly.
        # PARSE_FAILED is ours, not the platform's, and also proves it answered.
        changed = self.state is not BreakerState.CLOSED or self.consecutive_trips > 0
        self.state = BreakerState.CLOSED
        self.paused_until = None
        self.consecutive_trips = 0
        self.consecutive_errors = 0
        return changed

    def _trip(self, now: datetime, reason: str) -> bool:
        self.consecutive_trips += 1
        self.consecutive_errors = 0
        pause = self.pauses_minutes[min(self.consecutive_trips, len(self.pauses_minutes)) - 1]
        self.state = BreakerState.OPEN
        self.paused_until = now + timedelta(minutes=pause)
        self.last_reason = reason
        return True
