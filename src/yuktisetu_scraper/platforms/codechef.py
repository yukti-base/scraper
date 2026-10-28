"""CodeChef, read from the public profile page.

There is no API, so this reads the HTML -- but deliberately NOT through CSS
selectors, which are exactly what a redesign breaks. Two independent sources on
the same page are read and cross-checked:

  EMBEDDED JSON   `var all_rating = [...]`, the data behind the rating graph.
                  One entry per rated contest, so it gives the current rating
                  (last entry), the peak, and the contest count. A redesign of
                  the page's look does not touch it.

  TEXT ANCHORS    The visible labels -- "Total Problems Solved: 632",
                  "No. of Contests Participated: 102", "(Highest Rating 3445)".
                  Read from the tag-stripped text, so a renamed class or a moved
                  <div> changes nothing as long as a human can still read it.

Where both sources give a figure they must AGREE. If they do not, the page has
changed in a way we do not understand and the result is PARSE_FAILED -- never a
guess. A wrong number is worse than a missing one here, because students see the
score it produces.

Problems solved exists only as a text anchor, so it is read from there alone.

Terms of service: CodeChef's terms prohibit scraping. Reading the public profile a
student submitted is a deliberate, accepted decision while access is negotiated --
see the README. Nothing here evades a block: a challenge page is reported as
RATE_LIMITED and the circuit breaker backs off.
"""

from __future__ import annotations

import html as html_lib
import json
import re

from ..models import ExtractionTier, Metrics, Platform
from .base import Adapter, Blocked, HandleNotFound

PROFILE = "https://www.codechef.com/users/{handle}"

_ALL_RATING = re.compile(r"var\s+all_rating\s*=\s*(\[.*?\])\s*;", re.S)
_SCRIPT_OR_STYLE = re.compile(r"<(script|style)\b.*?</\1>", re.S | re.I)
_TAG = re.compile(r"<[^>]+>")
_SPACE = re.compile(r"\s+")

_SOLVED = re.compile(r"Total Problems Solved:\s*(\d+)", re.I)
_CONTESTS = re.compile(r"No\.\s*of Contests Participated:\s*(\d+)", re.I)
_HIGHEST = re.compile(r"\(\s*Highest Rating\s+(\d+)\s*\)", re.I)
# The rank block puts the value BEFORE its label ("1234 Global Rank"); the contest
# list puts it after ("Global Rank: 16975"). The negative lookahead keeps the
# per-contest figures out.
_GLOBAL_RANK = re.compile(r"(\d+|Inactive|NA)\s+Global Rank(?!\s*:)", re.I)
_STARS = re.compile(r"(\d)\s*★")
# The first rating-number block on the page is the main contest rating; the DSA
# block that follows reads "NA" for almost everyone.
_RATING_NUMBER = re.compile(r'class="rating-number"[^>]*>\s*([0-9]+|NA)', re.I)


class CodeChefAdapter(Adapter):
    platform = Platform.CODECHEF
    tier = ExtractionTier.EMBEDDED_JSON

    async def fetch(self, handle: str) -> tuple[Metrics, dict, ExtractionTier]:
        await self.throttle()
        resp = await self._client.get(
            PROFILE.format(handle=handle),
            headers={"Accept": "text/html"},
            # Redirects are read, not followed: CodeChef answers a handle that
            # does not exist with a 302 to its home page, and following it would
            # hand the parser the home page instead of telling us "not found".
            follow_redirects=False,
        )

        if resp.is_redirect:
            location = resp.headers.get("location", "")
            if f"/users/{handle.lower()}" in location.lower():
                # A canonicalising redirect (case, trailing slash) to the same
                # profile. Follow exactly that one hop.
                await self.throttle()
                resp = await self._client.get(location, headers={"Accept": "text/html"})
            else:
                raise HandleNotFound(f"codechef: no user '{handle}' (redirected to {location or 'home'})")

        if _is_challenge(resp):
            raise Blocked("codechef: Cloudflare challenge page instead of the profile")

        resp.raise_for_status()
        return parse_profile(resp.text, handle)


def parse_profile(page: str, handle: str) -> tuple[Metrics, dict, ExtractionTier]:
    """Pure, so the golden fixtures test exactly what production runs."""
    text = _page_text(page)

    if not _SOLVED.search(text) and "all_rating" not in page:
        # Neither source is present at all: this is not a profile page.
        raise ValueError("page has neither the rating data nor the profile labels")

    history = _rating_history(page)
    anchors = {
        "solved": _int(_SOLVED, text),
        "contests": _int(_CONTESTS, text),
        "highest": _int(_HIGHEST, text),
        "rating": _int(_RATING_NUMBER, page),
    }

    metrics = Metrics()
    metrics.solved_total = anchors["solved"]

    if history is not None:
        json_contests = len(history)
        json_rating = int(history[-1]["rating"]) if history else None
        json_peak = max(int(h["rating"]) for h in history) if history else None

        _must_agree("contest count", json_contests, anchors["contests"])
        _must_agree("current rating", json_rating, anchors["rating"])
        _must_agree("highest rating", json_peak, anchors["highest"])

        metrics.contests_attended = json_contests
        metrics.rating = json_rating
        metrics.max_rating = json_peak
        tier = ExtractionTier.EMBEDDED_JSON
    else:
        # The graph data has gone. The labels still carry the same facts, so use
        # them -- and record the lower tier, so a platform quietly sliding down
        # the ladder shows up in the data and not only in a log.
        metrics.contests_attended = anchors["contests"]
        metrics.max_rating = anchors["highest"]
        metrics.rating = anchors["rating"] if anchors["contests"] else None
        tier = ExtractionTier.SEMANTIC_ANCHOR

    # An account that has never entered a rated contest has NO rating. CodeChef
    # prints a starting figure for it; storing that would rank a beginner as if
    # they had competed.
    if metrics.contests_attended == 0:
        metrics.rating = None
        metrics.max_rating = None

    rank = _GLOBAL_RANK.search(text)
    if rank and rank.group(1).isdigit():
        metrics.global_rank = int(rank.group(1))

    stars = _STARS.search(text)
    if stars and metrics.contests_attended:
        metrics.stars = int(stars.group(1))

    raw = {
        "handle": handle,
        "tier": tier.value,
        "anchors": anchors,
        "rating_history": history,
    }
    return metrics, raw, tier


def _is_challenge(resp) -> bool:
    if resp.headers.get("cf-mitigated", "").lower() == "challenge":
        return True
    if resp.status_code in (403, 503) and "cloudflare" in resp.headers.get("server", "").lower():
        return True
    head = resp.text[:4000]
    return "Just a moment..." in head or "challenge-platform" in head


def _page_text(page: str) -> str:
    text = _TAG.sub(" ", _SCRIPT_OR_STYLE.sub(" ", page))
    return _SPACE.sub(" ", html_lib.unescape(text))


def _rating_history(page: str) -> list[dict] | None:
    m = _ALL_RATING.search(page)
    if not m:
        return None
    entries = json.loads(m.group(1))
    if not isinstance(entries, list) or any("rating" not in e for e in entries):
        raise ValueError("all_rating is present but not in the shape we read")
    return entries


def _int(pattern: re.Pattern, source: str) -> int | None:
    m = pattern.search(source)
    if not m or not m.group(1).isdigit():
        return None
    return int(m.group(1))


def _must_agree(what: str, from_json: int | None, from_text: int | None) -> None:
    if from_json is not None and from_text is not None and from_json != from_text:
        raise ValueError(
            f"codechef {what} disagrees between the rating data ({from_json}) "
            f"and the page text ({from_text}); refusing to guess"
        )
