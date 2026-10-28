"""The lane loop itself, against a fake store: whatever the wake time, it sleeps.

The real incident: paused, with a job already due -> "paused until <past time>"
765,000 times in four minutes. These run the real Lane.run for 1.5 s and count
the cycles."""
import asyncio
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from yuktisetu_scraper import lane as lane_mod
from yuktisetu_scraper import store
from yuktisetu_scraper.models import Platform
from yuktisetu_scraper.policy import BreakerState
from yuktisetu_scraper.ratelimit import RateLimiter

PAST = datetime.now(timezone.utc) - timedelta(minutes=3)


async def run_for(seconds, monkeypatch, *, paused):
    cycles = {"n": 0}

    async def load_breaker(platform, breaker):
        if paused:
            breaker.state = BreakerState.OPEN
            breaker.paused_until = datetime.now(timezone.utc) + timedelta(minutes=5)

    async def expire_overdue(platform):
        cycles["n"] += 1
        return []

    async def none(*a, **k):
        return None

    async def past(*a, **k):
        return PAST

    monkeypatch.setattr(store, "load_breaker", load_breaker)
    monkeypatch.setattr(store, "expire_overdue", expire_overdue)
    monkeypatch.setattr(store, "claim_one", none)
    monkeypatch.setattr(store, "next_wake_at", past)        # idle path: due "in the past"
    monkeypatch.setattr(store, "next_give_up_at", past)     # paused path: limit "in the past"
    monkeypatch.setattr(store, "save_breaker", none)

    async with httpx.AsyncClient() as client:
        ln = lane_mod.PlatformLane(Platform.CODECHEF, client, RateLimiter(lambda p: 0.5))
        task = asyncio.create_task(ln.run())
        await asyncio.sleep(seconds)
        ln.stop()
        await asyncio.wait_for(task, timeout=5)
    return cycles["n"]


@pytest.mark.asyncio
async def test_paused_lane_with_a_due_job_does_not_spin(monkeypatch):
    assert await run_for(1.5, monkeypatch, paused=True) <= 3


@pytest.mark.asyncio
async def test_idle_lane_with_a_past_wake_time_does_not_spin(monkeypatch):
    assert await run_for(1.5, monkeypatch, paused=False) <= 5
