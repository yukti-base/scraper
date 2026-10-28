"""The rules that bound every wait the drive pipeline can have on this service.

Pure functions, so "a job can never run past its limit" is tested directly
instead of by waiting an hour.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
import pytest

from yuktisetu_scraper.models import SnapshotStatus
from yuktisetu_scraper.platforms.codeforces import CodeforcesAdapter
from yuktisetu_scraper.policy import Breaker, BreakerState, after_failure

T0 = datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc)
LIMIT = T0 + timedelta(minutes=70)
OFFSETS = [15, 60]


def decide(status, attempts, now):
    return after_failure(
        status=status, attempts_done=attempts, created_at=T0, give_up_at=LIMIT,
        offsets_minutes=OFFSETS, now=now,
    )


# ------------------------------------------------------------------ retries


def test_agreed_schedule_retry_at_15_then_60_then_give_up():
    first = decide(SnapshotStatus.PLATFORM_ERROR, 1, T0 + timedelta(seconds=3))
    assert first.retry_at == T0 + timedelta(minutes=15)

    second = decide(SnapshotStatus.PLATFORM_ERROR, 2, T0 + timedelta(minutes=15, seconds=2))
    assert second.retry_at == T0 + timedelta(minutes=60)

    third = decide(SnapshotStatus.PLATFORM_ERROR, 3, T0 + timedelta(minutes=60, seconds=2))
    assert third.retry_at is None


def test_schedule_is_measured_from_queueing_so_a_slow_queue_cannot_stretch_it():
    """The first attempt ran 10 minutes late. The retry is still at +15 from
    queueing, not +15 from the late failure."""
    late = decide(SnapshotStatus.PLATFORM_ERROR, 1, T0 + timedelta(minutes=10))
    assert late.retry_at == T0 + timedelta(minutes=15)


def test_a_retry_that_would_pass_the_limit_ends_the_job_now():
    """A paused platform held the 2nd attempt until +65. Its retry would be after
    the limit, so the job ends and the student goes to TnP review."""
    late = decide(SnapshotStatus.RATE_LIMITED, 2, T0 + timedelta(minutes=71))
    assert late.retry_at is None
    assert "limit" in late.reason


@pytest.mark.parametrize("status", [SnapshotStatus.HANDLE_NOT_FOUND, SnapshotStatus.PARSE_FAILED])
def test_problems_retrying_cannot_fix_end_immediately(status):
    assert decide(status, 1, T0).retry_at is None


def test_overdue_retry_is_scheduled_for_now_not_the_past():
    d = decide(SnapshotStatus.PLATFORM_ERROR, 1, T0 + timedelta(minutes=20))
    assert d.retry_at == T0 + timedelta(minutes=20)


# ------------------------------------------------------------------ breaker


def breaker():
    return Breaker(pauses_minutes=[5, 10, 20, 40], error_threshold=3)


def test_first_block_pauses_the_platform_for_five_minutes():
    b = breaker()
    assert b.record(SnapshotStatus.RATE_LIMITED, T0)
    assert b.state is BreakerState.OPEN
    assert b.paused_until == T0 + timedelta(minutes=5)
    assert not b.allows_request(T0 + timedelta(minutes=4))


def test_after_the_pause_exactly_one_probe_and_success_closes_it():
    b = breaker()
    b.record(SnapshotStatus.RATE_LIMITED, T0)
    assert b.allows_request(T0 + timedelta(minutes=5))
    assert b.state is BreakerState.HALF_OPEN
    b.record(SnapshotStatus.OK, T0 + timedelta(minutes=5))
    assert b.state is BreakerState.CLOSED
    assert b.consecutive_trips == 0


def test_repeated_blocks_back_off_and_cap():
    b = breaker()
    pauses = []
    now = T0
    for _ in range(6):
        b.record(SnapshotStatus.RATE_LIMITED, now)
        pauses.append(int((b.paused_until - now).total_seconds() // 60))
        now = b.paused_until
        b.allows_request(now)
    assert pauses == [5, 10, 20, 40, 40, 40]


def test_a_platform_that_is_down_pauses_after_three_errors_not_one():
    b = breaker()
    assert not b.record(SnapshotStatus.PLATFORM_ERROR, T0)
    assert not b.record(SnapshotStatus.PLATFORM_ERROR, T0)
    assert b.record(SnapshotStatus.PLATFORM_ERROR, T0)
    assert b.state is BreakerState.OPEN


def test_a_failed_probe_pauses_again_straight_away():
    b = breaker()
    b.record(SnapshotStatus.RATE_LIMITED, T0)
    b.allows_request(T0 + timedelta(minutes=5))
    b.record(SnapshotStatus.PLATFORM_ERROR, T0 + timedelta(minutes=5))
    assert b.state is BreakerState.OPEN


def test_a_bad_handle_proves_the_platform_is_answering():
    b = breaker()
    b.record(SnapshotStatus.PLATFORM_ERROR, T0)
    b.record(SnapshotStatus.HANDLE_NOT_FOUND, T0)
    assert b.consecutive_errors == 0


# --------------------------------------------------------------- pacing


@pytest.mark.asyncio
async def test_codeforces_paces_each_of_its_two_calls(fixture):
    """Two API calls per student. Pacing once per student would fire them back to
    back, outside the published one-call-per-two-seconds limit."""
    info, rating = fixture("codeforces_user_info.json"), fixture("codeforces_user_rating.json")
    waited = []

    async def throttle():
        waited.append(1)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=info if "user.info" in request.url.path else rating)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
        await CodeforcesAdapter(c, throttle).scrape("someone")

    assert len(waited) == 2


@pytest.mark.asyncio
async def test_codeforces_call_limit_message_is_a_block_not_a_parse_failure():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "FAILED", "comment": "Call limit exceeded"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
        result = await CodeforcesAdapter(c).scrape("someone")

    assert result.status is SnapshotStatus.RATE_LIMITED
