"""Per-platform request pacing."""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict

from .models import Platform


class RateLimiter:
    """One token-bucket-ish gate per platform, shared by every worker task.

    Deliberately per-platform rather than global: LeetCode tolerates a faster
    pace than CodeChef, which should be read far more gently. A single global
    limit would force every platform down to the most fragile one's pace.

    This is a simple minimum-interval spacer, not a burst bucket. Bursting is
    exactly what gets an IP blocked, and smoothing costs little: the apply path
    never waits on a scrape, so a short queue at a deadline only delays when a
    capture lands, never whether a student can apply.
    """

    #: How far a platform can be slowed after repeated blocks: 8x its base gap.
    MAX_SLOWDOWN = 8
    #: Clean answers in a row before the gap is halved back towards normal.
    RECOVER_AFTER = 10

    def __init__(self, rate_for) -> None:
        self._rate_for = rate_for
        self._next_allowed: dict[Platform, float] = defaultdict(float)
        self._locks: dict[Platform, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._slowdown: dict[Platform, int] = defaultdict(lambda: 1)
        self._clean: dict[Platform, int] = defaultdict(int)

    def slowdown(self, platform: Platform) -> int:
        return self._slowdown[platform]

    def interval_for(self, platform: Platform) -> float:
        rate = self._rate_for(platform)
        return (1.0 / rate if rate > 0 else 0.0) * self._slowdown[platform]

    def slow_down(self, platform: Platform) -> int:
        """The platform blocked us: double the gap between its requests (capped).

        Resuming at the pace that just got us blocked would only earn the next
        block sooner. This is the gentler half of backing off; the breaker's
        pause is the other."""
        self._slowdown[platform] = min(self.MAX_SLOWDOWN, self._slowdown[platform] * 2)
        self._clean[platform] = 0
        return self._slowdown[platform]

    def record_clean(self, platform: Platform) -> int:
        """A normal answer. After RECOVER_AFTER in a row, halve the slowdown."""
        if self._slowdown[platform] == 1:
            return 1
        self._clean[platform] += 1
        if self._clean[platform] >= self.RECOVER_AFTER:
            self._slowdown[platform] = max(1, self._slowdown[platform] // 2)
            self._clean[platform] = 0
        return self._slowdown[platform]

    async def acquire(self, platform: Platform) -> None:
        interval = self.interval_for(platform)
        # Held across the sleep on purpose: the lock serialises this platform's
        # requests, which is what makes the spacing real rather than advisory.
        async with self._locks[platform]:
            now = time.monotonic()
            wait = self._next_allowed[platform] - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = time.monotonic()
            self._next_allowed[platform] = now + interval
