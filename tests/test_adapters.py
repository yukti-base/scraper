"""Golden-fixture tests for the API-backed adapters.

These are what catch a platform changing the shape of its response. They never
touch the network: the fixtures are saved real payloads, so a failure here means
our parser broke, not that a platform is having a bad day. The live checks live
in canary.py and are opt-in.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import httpx
import pytest

from yuktisetu_scraper.models import ExtractionTier, Metrics, Platform, SnapshotStatus
from yuktisetu_scraper.platforms.codeforces import CodeforcesAdapter
from yuktisetu_scraper.platforms.leetcode import LeetCodeAdapter


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# --------------------------------------------------------------- Codeforces


@pytest.mark.asyncio
async def test_codeforces_parses_rated_user(fixture):
    info, rating = fixture("codeforces_user_info.json"), fixture("codeforces_user_rating.json")

    def handler(request: httpx.Request) -> httpx.Response:
        if "user.info" in request.url.path:
            return httpx.Response(200, json=info)
        return httpx.Response(200, json=rating)

    async with _client(handler) as c:
        result = await CodeforcesAdapter(c).scrape("yuktisetu_canary")

    assert result.status is SnapshotStatus.OK
    assert result.tier is ExtractionTier.OFFICIAL_API
    assert result.metrics.rating == 1687
    assert result.metrics.max_rating == 1802
    assert result.metrics.contests_attended == 3


@pytest.mark.asyncio
async def test_codeforces_unrated_user_yields_nulls_not_zeros(fixture):
    """A student who never entered a contest must not look like a rating of 0.

    This is the most important distinction in the file, and the two halves differ:

      rating / max_rating -- Codeforces omits these keys ENTIRELY for an account
        that has never been rated, so they are a genuine ABSENCE and must stay
        None. Defaulting them to 0 would drop the student to the bottom of a
        percentile instead of excluding them from it.

      contests_attended -- an empty contest history is a MEASURED ZERO. We asked
        and the true answer is "none". 0 is correct here and must survive; turning
        it into None would throw away a real fact about the student.

    So this snapshot is a legitimate OK, not a parse failure: it accurately
    describes a beginner.
    """
    info = fixture("codeforces_unrated_user.json")

    def handler(request: httpx.Request) -> httpx.Response:
        if "user.info" in request.url.path:
            return httpx.Response(200, json=info)
        return httpx.Response(200, json={"status": "OK", "result": []})

    async with _client(handler) as c:
        result = await CodeforcesAdapter(c).scrape("yuktisetu_fresher")

    assert result.status is SnapshotStatus.OK
    assert result.metrics.rating is None
    assert result.metrics.max_rating is None
    assert result.metrics.contests_attended == 0


@pytest.mark.asyncio
async def test_codeforces_bad_handle_is_not_retryable(fixture):
    body = fixture("codeforces_bad_handle.json")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json=body)

    async with _client(handler) as c:
        result = await CodeforcesAdapter(c).scrape("nosuchuser_zzz")

    assert result.status is SnapshotStatus.HANDLE_NOT_FOUND
    assert not result.status.is_retryable


@pytest.mark.asyncio
async def test_codeforces_contest_history_failure_keeps_the_rating(fixture):
    """Losing the softer second call must not lose the whole snapshot."""
    info = fixture("codeforces_user_info.json")

    def handler(request: httpx.Request) -> httpx.Response:
        if "user.info" in request.url.path:
            return httpx.Response(200, json=info)
        return httpx.Response(503, text="service unavailable")

    async with _client(handler) as c:
        result = await CodeforcesAdapter(c).scrape("yuktisetu_canary")

    assert result.status is SnapshotStatus.OK
    assert result.metrics.rating == 1687
    assert result.metrics.contests_attended is None


@pytest.mark.asyncio
async def test_codeforces_rate_limit_is_retryable():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, text="Call limit exceeded")

    async with _client(handler) as c:
        result = await CodeforcesAdapter(c).scrape("anyone")

    assert result.status is SnapshotStatus.RATE_LIMITED
    assert result.status.is_retryable


# ----------------------------------------------------------------- LeetCode


@pytest.mark.asyncio
async def test_leetcode_parses_full_profile(fixture):
    body = fixture("leetcode_matched_user.json")

    async with _client(lambda r: httpx.Response(200, json=body)) as c:
        result = await LeetCodeAdapter(c).scrape("yuktisetu_canary")

    assert result.status is SnapshotStatus.OK
    assert result.tier is ExtractionTier.STRUCTURED_API
    m = result.metrics
    assert m.solved_total == 742
    assert (m.solved_easy, m.solved_medium, m.solved_hard) == (310, 358, 74)
    # 1842.47 rounds to an int; sub-point precision is meaningless for scoring.
    assert m.rating == 1842
    assert m.contests_attended == 31
    assert m.top_percent == pytest.approx(8.4)
    # Contest ranking overrides the profile ranking when both are present.
    assert m.global_rank == 41203
    assert set(m.languages) == {"Java", "Python3"}
    assert m.topics["Dynamic Programming"] == 88
    assert m.topics["Array"] == 300


@pytest.mark.asyncio
async def test_leetcode_without_contests_keeps_solve_counts(fixture):
    """userContestRanking is null for most students. Not an error."""
    body = fixture("leetcode_no_contests.json")

    async with _client(lambda r: httpx.Response(200, json=body)) as c:
        result = await LeetCodeAdapter(c).scrape("yuktisetu_fresher")

    assert result.status is SnapshotStatus.OK
    assert result.metrics.solved_total == 12
    assert result.metrics.rating is None
    assert result.metrics.contests_attended is None
    assert result.metrics.global_rank == 998877


@pytest.mark.asyncio
async def test_leetcode_missing_user_is_handle_not_found(fixture):
    """GraphQL reports this as a 200 with an errors array, not a 404."""
    body = fixture("leetcode_no_such_user.json")

    async with _client(lambda r: httpx.Response(200, json=body)) as c:
        result = await LeetCodeAdapter(c).scrape("nosuchuser_zzz")

    assert result.status is SnapshotStatus.HANDLE_NOT_FOUND
    assert not result.status.is_retryable


@pytest.mark.asyncio
async def test_leetcode_sends_a_referer():
    """LeetCode 403s a request without one, so this is behaviour, not decoration."""
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(request.headers)
        return httpx.Response(200, json={"data": {"matchedUser": None}, "errors":
                                         [{"message": "That user does not exist."}]})

    async with _client(handler) as c:
        await LeetCodeAdapter(c).scrape("someone")

    assert "referer" in {k.lower() for k in seen}


# ------------------------------------------------------------------- shared


def test_metrics_is_empty_detects_a_silently_broken_parser():
    assert Metrics().is_empty()
    assert not Metrics(rating=1500).is_empty()
    assert not Metrics(solved_total=1).is_empty()
    # A zero is NOT emptiness: a genuine 0 hard problems solved is a real value
    # and must survive, which is why is_empty() tests for None and not falsiness.
    assert not Metrics(solved_hard=0, solved_total=10).is_empty()


def test_only_outages_are_retryable():
    """The retry schedule and the TnP hold both key off this.

    A handle that does not exist and a parser that is broken must never enter the
    retry-and-hold path: one is the student's to fix and the other is ours, and
    neither improves by waiting.
    """
    assert SnapshotStatus.PLATFORM_ERROR.is_retryable
    assert SnapshotStatus.RATE_LIMITED.is_retryable
    assert not SnapshotStatus.HANDLE_NOT_FOUND.is_retryable
    assert not SnapshotStatus.PARSE_FAILED.is_retryable
    assert not SnapshotStatus.OK.is_retryable
