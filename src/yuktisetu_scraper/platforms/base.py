"""The adapter contract."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

import httpx

from ..models import ExtractionTier, Metrics, Platform, ScrapeResult, SnapshotStatus

log = logging.getLogger(__name__)


class Adapter(ABC):
    """One platform's reader.

    Subclasses implement fetch(), which may raise freely -- scrape() below turns
    anything that escapes into a ScrapeResult. That is where this service's
    promise is actually kept: a caller never sees an exception and never sees
    nothing, only a result with a status it can branch on.
    """

    platform: Platform
    tier: ExtractionTier

    def __init__(
        self,
        client: httpx.AsyncClient,
        throttle: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._client = client
        # Awaited before EVERY request, not once per scrape. Codeforces needs two
        # calls per student, and pacing per student would send them back to back
        # -- inside the platform's published limit on average, outside it at the
        # moment it matters.
        self._throttle = throttle or _no_throttle

    async def throttle(self) -> None:
        await self._throttle()

    @abstractmethod
    async def fetch(self, handle: str) -> tuple:
        """Return (metrics, raw) or (metrics, raw, tier).

        Raise HandleNotFound if the profile is absent and Blocked if the platform
        refused to answer."""

    async def scrape(self, handle: str) -> ScrapeResult:
        now = datetime.now(timezone.utc)
        base = dict(platform=self.platform, handle=handle, scraped_at=now)

        try:
            fetched = await self.fetch(handle)
            # An adapter with a fallback ladder reports which rung it used as a
            # third element; a single-source adapter returns the usual pair.
            metrics, raw, tier = fetched if len(fetched) == 3 else (*fetched, self.tier)
        except Blocked as e:
            # The platform refused us: a Cloudflare challenge, a "call limit
            # exceeded". Retryable, and the signal that trips the circuit breaker,
            # so it must never be mistaken for a parse failure -- that is terminal
            # and would send a student to TnP review for our own impatience.
            log.warning("%s refused the request for %s: %s", self.platform.value, handle, e)
            return ScrapeResult(**base, status=SnapshotStatus.RATE_LIMITED, error=str(e))
        except HandleNotFound as e:
            # The student's problem, and they can fix it. Never retried, because
            # retrying a handle that does not exist just wastes the rate budget.
            log.info("%s handle not found: %s", self.platform.value, handle)
            return ScrapeResult(**base, status=SnapshotStatus.HANDLE_NOT_FOUND, error=str(e))
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            # 429 and 403 are the shapes a block takes. Distinguished from a plain
            # 5xx because they mean "back off harder", not "try again shortly".
            status = (
                SnapshotStatus.RATE_LIMITED
                if code in (429, 403)
                else SnapshotStatus.PLATFORM_ERROR
            )
            log.warning("%s HTTP %s for %s", self.platform.value, code, handle)
            return ScrapeResult(**base, status=status, error=f"HTTP {code}")
        except (httpx.TimeoutException, httpx.TransportError) as e:
            log.warning("%s transport failure for %s: %s", self.platform.value, handle, e)
            return ScrapeResult(
                **base, status=SnapshotStatus.PLATFORM_ERROR, error=f"{type(e).__name__}: {e}"
            )
        except Exception as e:  # noqa: BLE001
            # A bug in OUR parser, not the platform's fault. Separated so that a
            # canary check can tell "the platform changed" from "the platform is
            # down" -- they need completely different responses from us.
            log.error("%s parse failure for %s: %s", self.platform.value, handle, e, exc_info=True)
            return ScrapeResult(
                **base, status=SnapshotStatus.PARSE_FAILED, error=f"{type(e).__name__}: {e}"
            )

        if metrics.is_empty():
            # Status OK with nothing in it is the most dangerous outcome there is:
            # it looks like success, so nothing alerts, and the student scores on
            # an empty snapshot. Treated as a parser failure, which it is.
            log.error(
                "%s returned no usable metrics for %s -- treating as a parse failure",
                self.platform.value,
                handle,
            )
            return ScrapeResult(
                **base,
                status=SnapshotStatus.PARSE_FAILED,
                raw=raw,
                error="adapter extracted no usable metrics",
            )

        log.debug(
            "%s ok handle=%s tier=%s rating=%s solved=%s contests=%s",
            self.platform.value,
            handle,
            tier.value,
            metrics.rating,
            metrics.solved_total,
            metrics.contests_attended,
        )
        return ScrapeResult(
            **base, status=SnapshotStatus.OK, tier=tier, metrics=metrics, raw=raw
        )


class HandleNotFound(Exception):
    """The platform answered, and says there is no such profile."""


class Blocked(Exception):
    """The platform answered with a refusal rather than data -- a challenge page,
    an explicit rate-limit message inside a 200."""


async def _no_throttle() -> None:
    return None
