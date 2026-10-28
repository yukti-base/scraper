"""The worker: one lane per platform, plus the LISTEN task that wakes them.

Event-driven end to end. Java's apply transaction inserts a coding_scrape_jobs
row; a trigger NOTIFYs on commit; the listener wakes that platform's lane, which
claims the job within milliseconds. Nothing polls the queue on an interval.
"""

from __future__ import annotations

import asyncio
import logging

import httpx
from psycopg import AsyncConnection

from . import store
from .config import settings
from .lane import PlatformLane
from .platforms import ADAPTERS
from .models import Platform
from .ratelimit import RateLimiter

log = logging.getLogger(__name__)

CHANNEL = "coding_scrape_jobs"


class Worker:
    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None
        self._limiter = RateLimiter(settings.rate_for)
        self.lanes: dict[Platform, PlatformLane] = {}
        self._tasks: list[asyncio.Task] = []
        self._stopping = asyncio.Event()
        self.listener_connected = False

    async def start(self) -> None:
        self._client = httpx.AsyncClient(
            timeout=settings.http_timeout_seconds,
            follow_redirects=True,
            headers={
                # Identifies us honestly and gives the platform somewhere to
                # complain to. A platform that wants us to stop should be able to
                # tell us, which is not possible behind a spoofed browser string.
                "User-Agent": (
                    "YuktiSetu-TnP/0.1 (PCET Training & Placement Cell; "
                    "reads student-submitted public profiles)"
                ),
                "Accept": "application/json",
            },
        )

        reclaimed = await store.reap_orphans()
        if reclaimed:
            log.warning("reclaimed %s job(s) left RUNNING by a previous process", reclaimed)

        closed = await store.close_blocked_jobs(settings.blocked_platforms)
        if closed:
            log.warning("closed %s job(s) for blocked platform(s) %s without reading them",
                        closed, sorted(p.value for p in settings.blocked_platforms))

        for platform in ADAPTERS:
            if platform in settings.blocked_platforms:
                log.info("lane %s NOT started: platform is blocked (terms of service)", platform.value)
                continue
            lane = PlatformLane(platform, self._client, self._limiter)
            self.lanes[platform] = lane
            self._tasks.append(asyncio.create_task(lane.run(), name=f"lane-{platform.value}"))
        self._tasks.append(asyncio.create_task(self._listen(), name="listen"))

        # States what is running at startup, deliberately: a platform with no
        # lane silently never scrapes, so "not built" and "working" must not look
        # the same in the log.
        log.info(
            "worker started: lanes=[%s] retry_offsets=%s min breaker_pauses=%s min",
            ", ".join(p.value for p in self.lanes), settings.retry_offsets, settings.breaker_pauses,
        )

    async def stop(self) -> None:
        self._stopping.set()
        for lane in self.lanes.values():
            lane.stop()
        if self._tasks:
            done, pending = await asyncio.wait(self._tasks, timeout=10)
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        if self._client:
            await self._client.aclose()
        log.info("worker stopped")

    async def _listen(self) -> None:
        """Hold a LISTEN connection, reconnecting with backoff if it drops.

        After every (re)connect all lanes are woken once, because notifications
        sent while we were disconnected are gone -- the jobs are not, and one
        sweep of each lane picks them up.
        """
        backoff = 1
        while not self._stopping.is_set():
            try:
                async with await AsyncConnection.connect(settings.db_url, autocommit=True) as conn:
                    await conn.execute(f"LISTEN {CHANNEL}")
                    self.listener_connected = True
                    backoff = 1
                    log.info("listening on %s", CHANNEL)
                    for lane in self.lanes.values():
                        lane.wake()

                    while not self._stopping.is_set():
                        # A bounded wait, so a stop request is noticed promptly
                        # and a dead connection surfaces as an error here.
                        async for note in conn.notifies(timeout=5.0):
                            lane = self._lane_for(note.payload)
                            if lane:
                                log.debug("notify: job queued for %s", note.payload)
                                lane.wake()
                        await conn.execute("SELECT 1")
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                self.listener_connected = False
                log.error("LISTEN connection lost (%s); reconnecting in %ss", e, backoff)
                try:
                    await asyncio.wait_for(self._stopping.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2, 30)
        self.listener_connected = False

    def _lane_for(self, payload: str) -> PlatformLane | None:
        try:
            return self.lanes.get(Platform(payload))
        except ValueError:
            return None
