"""Pre-flight check against known-good profiles.

The single most valuable control in this service. Before a batch runs, scrape a
handful of real accounts whose figures we know; if one no longer parses, or moves
implausibly, ABORT rather than writing thousands of wrong rows.

A silently-broken parser is far worse than an outage: an outage is loud and
recoverable, while a parser that returns plausible nonsense corrupts scores that
students have already been shown.

Run manually, or from a nightly job:
    python -m yuktisetu_scraper.canary
"""

from __future__ import annotations

import asyncio
import logging
import sys

import httpx

from .config import settings
from .models import Platform
from .platforms import adapter_for

log = logging.getLogger("canary")

# Real, long-established accounts. Chosen because they are stable and public;
# the assertions are deliberately loose floors rather than exact values, since
# the real people behind them keep solving problems.
CANARIES: list[tuple[Platform, str, dict]] = [
    (Platform.CODEFORCES, "tourist", {"min_rating": 3000, "min_contests": 50}),
    (Platform.LEETCODE, "votrubac", {"min_solved": 1000}),
]


async def run() -> int:
    failures: list[str] = []

    async with httpx.AsyncClient(
        timeout=settings.http_timeout_seconds,
        follow_redirects=True,
        headers={"User-Agent": "YuktiSetu-TnP/0.1 (canary check)"},
    ) as client:
        for platform, handle, expect in CANARIES:
            adapter = adapter_for(platform, client)
            if adapter is None:
                continue

            result = await adapter.scrape(handle)
            if not result.ok:
                failures.append(
                    f"{platform.value}/{handle}: status={result.status.value} error={result.error}"
                )
                continue

            m = result.metrics
            if "min_rating" in expect and (m.rating or 0) < expect["min_rating"]:
                failures.append(
                    f"{platform.value}/{handle}: rating {m.rating} below floor {expect['min_rating']}"
                )
            if "min_contests" in expect and (m.contests_attended or 0) < expect["min_contests"]:
                failures.append(
                    f"{platform.value}/{handle}: contests {m.contests_attended} "
                    f"below floor {expect['min_contests']}"
                )
            if "min_solved" in expect and (m.solved_total or 0) < expect["min_solved"]:
                failures.append(
                    f"{platform.value}/{handle}: solved {m.solved_total} "
                    f"below floor {expect['min_solved']}"
                )

            log.info(
                "canary ok %s/%s rating=%s solved=%s contests=%s tier=%s",
                platform.value, handle, m.rating, m.solved_total,
                m.contests_attended, result.tier.value if result.tier else None,
            )

    if failures:
        for f in failures:
            log.error("CANARY FAILED %s", f)
        return 1
    log.info("all %s canaries passed", len(CANARIES))
    return 0


if __name__ == "__main__":
    from .logging_setup import configure

    configure("INFO")
    sys.exit(asyncio.run(run()))
