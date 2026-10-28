"""Codeforces, via its official public REST API.

The only platform here with a documented, supported contract: no key, no
scraping, a published limit of one call per two seconds, and an explicit
{status, result} envelope.
https://codeforces.com/apiHelp

Two calls, deliberately not three:
  user.info    -- rating, maxRating, rank
  user.rating  -- the contest history, whose length is the contest count

user.status would give solved counts, but it returns EVERY submission the user
has ever made -- tens of thousands of rows for an active competitor, paginated,
and several seconds per student. At 400 applicants that is the slowest thing in
the whole pipeline for one metric that Codeforces' own rating already reflects.
Left out on purpose; solved_total stays None and the percentile handles it.
"""

from __future__ import annotations

from ..models import ExtractionTier, Metrics, Platform
from .base import Adapter, Blocked, HandleNotFound

API = "https://codeforces.com/api"


class CodeforcesAdapter(Adapter):
    platform = Platform.CODEFORCES
    tier = ExtractionTier.OFFICIAL_API

    async def fetch(self, handle: str) -> tuple[Metrics, dict]:
        info = await self._call("user.info", {"handles": handle, "checkHistoricHandles": "false"})
        users = info.get("result") or []
        if not users:
            raise HandleNotFound(f"codeforces: no user '{handle}'")
        user = users[0]

        # A user who has never entered a rated contest has no rating fields at
        # all -- the keys are simply absent rather than zero. .get() is correct
        # here and a default of 0 would be a lie.
        metrics = Metrics(
            rating=user.get("rating"),
            max_rating=user.get("maxRating"),
        )

        # Contest history is a separate call and a softer requirement: if it
        # fails we still have a usable rating, so losing it must not lose the
        # whole snapshot.
        try:
            rating_history = await self._call("user.rating", {"handle": handle})
            contests = rating_history.get("result") or []
            metrics.contests_attended = len(contests)
            if contests and metrics.max_rating is None:
                # Fallback for the odd account whose user.info omits maxRating
                # but whose history plainly contains it.
                metrics.max_rating = max(c.get("newRating", 0) for c in contests) or None
        except Exception:  # noqa: BLE001
            rating_history = {"result": None, "note": "contest history unavailable"}

        raw = {"user_info": user, "user_rating_count": metrics.contests_attended}
        return metrics, raw

    async def _call(self, method: str, params: dict) -> dict:
        await self.throttle()
        resp = await self._client.get(f"{API}/{method}", params=params)

        # Codeforces answers a bad handle with HTTP 400 and a comment saying so,
        # which is a different thing from the API being broken. Reading the
        # comment is what keeps "this student typed it wrong" out of the
        # retry-and-hold path meant for genuine outages.
        if resp.status_code == 400:
            comment = ""
            try:
                comment = (resp.json() or {}).get("comment", "")
            except Exception:  # noqa: BLE001
                pass
            if "not found" in comment.lower() or "handles" in comment.lower():
                raise HandleNotFound(f"codeforces: {comment or 'bad handle'}")

        resp.raise_for_status()
        body = resp.json()
        if body.get("status") != "OK":
            comment = body.get("comment", "")
            if "not found" in comment.lower():
                raise HandleNotFound(f"codeforces: {comment}")
            if "limit" in comment.lower():
                # "Call limit exceeded". Retryable and a breaker trip -- treating it
                # as a parse failure would make it terminal and flag a student for
                # TnP review because WE asked too fast.
                raise Blocked(f"codeforces: {comment}")
            raise RuntimeError(f"codeforces api status={body.get('status')} comment={comment}")
        return body
