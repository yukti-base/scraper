"""One platform's lane: its own queue, rate gate, counters and circuit breaker.

Why one lane per platform rather than one shared worker:
  * Isolation. A CodeChef backlog of 300 or a CodeChef block never delays a
    LeetCode job -- they are different queues drained by different tasks.
  * Each platform's politeness is enforced in one place. A lane sends at most
    one request at a time, paced by its own rate.

Why nothing here can hang:
  * The lane SLEEPS until exactly the next thing it owes -- a NOTIFY for a new
    job, the next retry coming due, a pending job reaching its limit, or the end
    of a breaker pause -- whichever is soonest. There is no busy loop and no
    fixed-interval poll doing work.
  * Every scrape runs under a hard timeout, so a hung read cannot pin the lane.
  * The loop catches everything and carries on. A lane that died silently would
    look exactly like an empty queue, which is the failure that hides for days.
  * No database connection is held across a network call.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone

import httpx

from . import store
from .config import settings
from .counters import RequestCounters
from .models import Platform, ScrapeResult, SnapshotStatus
from .platforms import adapter_for
from .policy import Breaker, BreakerState, after_failure
from .ratelimit import RateLimiter

log = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


MIN_PAUSED_SLEEP_SECONDS = 1.0
#: The shortest any lane sleep may be, on every path. A computed wake time can
#: always end up in the past (a due job the lane may not take, the app's clock a
#: little ahead of the database's) and a zero sleep is a busy loop.
MIN_SLEEP_SECONDS = 0.5
#: Spin guard: this many cycles in a row that ran no job and took under
#: FAST_CYCLE_SECONDS each means something is computing a zero sleep.
SPIN_CYCLES = 20
FAST_CYCLE_SECONDS = 0.05
SPIN_BACKOFF_SECONDS = 5.0


def paused_sleep_seconds(paused_until: datetime | None, give_up_at: datetime | None,
                         now: datetime, cap: float) -> float:
    """How long a paused lane sleeps: until the pause ends or a pending job
    reaches its limit, whichever is sooner, capped -- and never less than a
    second, so a time already in the past can never become a busy loop."""
    candidates = [t for t in (paused_until, give_up_at) if t is not None]
    if not candidates:
        return cap
    seconds = (min(candidates) - now).total_seconds()
    return max(MIN_PAUSED_SLEEP_SECONDS, min(seconds, cap))


class PlatformLane:
    def __init__(self, platform: Platform, client: httpx.AsyncClient, limiter: RateLimiter) -> None:
        self.platform = platform
        self._client = client
        self._limiter = limiter
        self._wake = asyncio.Event()
        self._stopping = asyncio.Event()
        self.counters = RequestCounters()
        self.breaker = Breaker(
            pauses_minutes=settings.breaker_pauses,
            error_threshold=settings.breaker_error_threshold,
        )
        self.last_cycle_at: datetime | None = None
        self.current_job: int | None = None
        self._ran_job = False

    # ------------------------------------------------------------ control

    def wake(self) -> None:
        """Called by the LISTEN task when a job for this platform is queued."""
        self._wake.set()

    def stop(self) -> None:
        self._stopping.set()
        self._wake.set()

    # --------------------------------------------------------------- loop

    async def run(self) -> None:
        await store.load_breaker(self.platform, self.breaker)
        log.info(
            "lane %s started: rate=%s/s breaker=%s",
            self.platform.value, settings.rate_for(self.platform), self.breaker.state.value,
        )
        failures_in_a_row = 0
        fast_idle_cycles = 0

        while not self._stopping.is_set():
            self.last_cycle_at = _now()
            started = time.monotonic()
            self._ran_job = False
            try:
                await self._cycle()
                failures_in_a_row = 0
                # The last line of defence against any zero-sleep bug, known or
                # not: a lane doing no work must not cycle flat out.
                if not self._ran_job and time.monotonic() - started < FAST_CYCLE_SECONDS:
                    fast_idle_cycles += 1
                    if fast_idle_cycles >= SPIN_CYCLES:
                        log.warning(
                            "lane %s: %s idle cycles in a row with no sleep -- backing off %ss "
                            "(a wake time is being computed in the past)",
                            self.platform.value, fast_idle_cycles, SPIN_BACKOFF_SECONDS,
                        )
                        fast_idle_cycles = 0
                        await self._sleep(SPIN_BACKOFF_SECONDS)
                else:
                    fast_idle_cycles = 0
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                # Never let the lane die. Back off briefly so a database outage
                # does not become a tight error loop, then try again.
                failures_in_a_row += 1
                delay = min(2 ** failures_in_a_row, 60)
                log.exception("lane %s cycle failed; retrying in %ss", self.platform.value, delay)
                await self._sleep(delay)

        log.info("lane %s stopped", self.platform.value)

    async def _cycle(self) -> None:
        # 1. Anything past its limit ends now, paused platform or not. This is
        #    what bounds every wait the drive pipeline can ever have on us.
        expired = await store.expire_overdue(self.platform)
        if expired:
            log.warning(
                "lane %s: %s job(s) reached their time limit and were failed for TnP review: %s",
                self.platform.value, len(expired), expired,
            )

        # 2. Paused: sleep until the pause ends or a pending job hits its limit.
        #    NOT until the next job is due: a due job cannot be taken while
        #    paused, and its available_at is already past, which made this a
        #    sleep of zero -- a busy loop of hundreds of thousands of cycles.
        now = _now()
        if not self.breaker.allows_request(now):
            give_up_at = await store.next_give_up_at(self.platform)
            seconds = paused_sleep_seconds(self.breaker.paused_until, give_up_at, now,
                                           settings.lane_idle_max_seconds)
            log.debug("lane %s paused until %s; next check in %.0fs",
                      self.platform.value, self.breaker.paused_until, seconds)
            await self._sleep(seconds)
            return
        if self.breaker.state is BreakerState.HALF_OPEN:
            log.info("lane %s: pause over, sending one probe request", self.platform.value)
            await self._persist_breaker()

        # 3. Work, one job at a time.
        #
        # The wake flag is cleared BEFORE looking for work, not before sleeping.
        # A NOTIFY that lands between "found nothing" and "go to sleep" then
        # leaves the flag set and the sleep returns at once -- clearing it later
        # would silently swallow that job until the idle cap.
        self._wake.clear()
        job = await store.claim_one(self.platform)
        if job is not None:
            await self._run_job(job)
            return

        # 4. Nothing due: sleep until the next retry or limit, or a NOTIFY.
        wake_at = await store.next_wake_at(self.platform)
        await self._sleep_until(wake_at, wake_on_notify=True)

    # --------------------------------------------------------------- jobs

    async def _run_job(self, job: dict) -> None:
        self.current_job = job["id"]
        self._ran_job = True
        try:
            log.debug(
                "lane %s job=%s student=%s handle=%s reason=%s attempt=%s",
                self.platform.value, job["id"], job["student_user_id"], job["handle"],
                job["reason"], job["attempts"],
            )

            fresh = await store.fresh_snapshot_id(job["student_user_id"], self.platform, job["handle"])
            if fresh is not None:
                await store.finish_with_fresh_snapshot(job, fresh)
                log.info(
                    "lane %s job=%s student=%s reused snapshot=%s (younger than %s min)",
                    self.platform.value, job["id"], job["student_user_id"], fresh,
                    settings.snapshot_fresh_minutes,
                )
                return

            adapter = adapter_for(
                self.platform, self._client, throttle=lambda: self._limiter.acquire(self.platform)
            )
            started = time.monotonic()
            try:
                result = await asyncio.wait_for(
                    adapter.scrape(job["handle"]), timeout=settings.scrape_hard_timeout_seconds
                )
            except asyncio.TimeoutError:
                result = ScrapeResult(
                    platform=self.platform,
                    handle=job["handle"],
                    status=SnapshotStatus.PLATFORM_ERROR,
                    error=f"no answer within {settings.scrape_hard_timeout_seconds}s",
                    scraped_at=_now(),
                )
            latency_ms = (time.monotonic() - started) * 1000
            self.counters.record(result.status, latency_ms)

            now = _now()
            breaker_changed = self.breaker.record(result.status, now)
            if result.status is SnapshotStatus.RATE_LIMITED:
                self._limiter.slow_down(self.platform)
            elif result.status in (SnapshotStatus.OK, SnapshotStatus.HANDLE_NOT_FOUND):
                self._limiter.record_clean(self.platform)
            if breaker_changed:
                if self.breaker.state is BreakerState.OPEN:
                    log.warning(
                        "lane %s PAUSED until %s: %s (trip %s); requests now %sx slower",
                        self.platform.value, self.breaker.paused_until.isoformat(),
                        self.breaker.last_reason, self.breaker.consecutive_trips,
                        self._limiter.slowdown(self.platform),
                    )
                else:
                    log.info("lane %s resumed: platform answering normally", self.platform.value)

            decision = (
                after_failure(
                    status=result.status,
                    attempts_done=job["attempts"],
                    created_at=job["created_at"],
                    give_up_at=job["give_up_at"],
                    offsets_minutes=settings.retry_offsets,
                    now=now,
                )
                if not result.ok
                else None
            )
            # The job's outcome is recorded FIRST. Saving the breaker used to
            # come before it, and when that save failed the job was left
            # RUNNING with its result lost.
            outcome = await store.record_attempt(
                job, result, decision.retry_at if decision else None
            )
            self._log_outcome(job, result, decision, outcome, latency_ms)
            if breaker_changed:
                await self._persist_breaker()
        finally:
            self.current_job = None

    async def _persist_breaker(self) -> None:
        """Save the breaker so a restart keeps the pause. Never fatal: the pause is
        already in force in memory, and a failed save must not stop the lane."""
        try:
            await store.save_breaker(self.platform, self.breaker)
        except Exception:  # noqa: BLE001
            log.exception("lane %s: could not save the breaker state; the pause holds in memory",
                          self.platform.value)

    def _log_outcome(self, job, result, decision, outcome, latency_ms) -> None:
        base = (
            f"lane {self.platform.value} job={job['id']} student={job['student_user_id']} "
            f"handle={job['handle']} attempt={job['attempts']} {latency_ms:.0f}ms"
        )
        if result.ok:
            m = result.metrics
            log.info(
                "%s CAPTURED snapshot=%s tier=%s rating=%s solved=%s contests=%s",
                base, outcome["snapshot_id"], result.tier.value if result.tier else None,
                m.rating, m.solved_total, m.contests_attended,
            )
        elif decision and decision.retry_at:
            log.warning(
                "%s %s, retry at %s: %s",
                base, result.status.value, decision.retry_at.isoformat(), result.error,
            )
        else:
            log.warning(
                "%s FAILED %s (%s), flagged for TnP review: %s",
                base, result.status.value, decision.reason if decision else "", result.error,
            )

    # -------------------------------------------------------------- sleep

    async def _sleep_until(self, at: datetime | None, *, wake_on_notify: bool) -> None:
        cap = settings.lane_idle_max_seconds
        seconds = cap if at is None else max(MIN_SLEEP_SECONDS, min((at - _now()).total_seconds(), cap))
        if wake_on_notify:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=seconds)
            except asyncio.TimeoutError:
                pass
        else:
            await self._sleep(seconds)

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stopping.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    # ------------------------------------------------------------- health

    def health(self) -> dict:
        b = self.breaker
        return {
            "breaker": b.state.value,
            "paused_until": b.paused_until.isoformat() if b.paused_until else None,
            # 1 = normal pace; 2, 4, 8 = slowed down after being blocked.
            "slowdown": self._limiter.slowdown(self.platform),
            "consecutive_trips": b.consecutive_trips,
            "last_reason": b.last_reason,
            "rate_per_second": settings.rate_for(self.platform),
            "current_job": self.current_job,
            "last_cycle_at": self.last_cycle_at.isoformat() if self.last_cycle_at else None,
            "requests": self.counters.snapshot(),
        }
