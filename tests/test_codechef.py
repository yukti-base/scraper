"""CodeChef, against two real saved pages and the ways the page can go wrong.

codechef_rated_veteran.html  a long-established public profile: 102 contests.
codechef_one_contest.html    a student-scale profile: one contest, 27 solved.

Neither test touches the network.
"""

from __future__ import annotations

import pathlib

import httpx
import pytest

from yuktisetu_scraper.models import ExtractionTier, SnapshotStatus
from yuktisetu_scraper.platforms.codechef import CodeChefAdapter, parse_profile

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


def page(name: str) -> str:
    return (FIXTURES / name).read_text()


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# ------------------------------------------------------------- real pages


def test_veteran_profile_reads_every_figure_and_both_sources_agree():
    metrics, raw, tier = parse_profile(page("codechef_rated_veteran.html"), "veteran")

    assert tier is ExtractionTier.EMBEDDED_JSON
    assert metrics.rating == 3355          # last entry of the rating history
    assert metrics.max_rating == 3445      # peak of the history == "Highest Rating"
    assert metrics.contests_attended == 102
    assert metrics.solved_total == 632
    assert metrics.stars == 7
    # The rank block reads "Inactive" for this account, and "Inactive" is not a
    # number -- it must stay None rather than becoming 0.
    assert metrics.global_rank is None
    assert raw["anchors"] == {"solved": 632, "contests": 102, "highest": 3445, "rating": 3355}


def test_student_scale_profile():
    metrics, _, tier = parse_profile(page("codechef_one_contest.html"), "student")

    assert tier is ExtractionTier.EMBEDDED_JSON
    assert metrics.rating == 776
    assert metrics.max_rating == 776
    assert metrics.contests_attended == 1
    assert metrics.solved_total == 27
    assert metrics.stars == 1


def test_per_contest_ranks_are_not_mistaken_for_the_global_rank():
    """The contest list says "Global Rank: 16975" for one contest. That is a
    placing in one round, not the account's standing, and must not be read."""
    metrics, _, _ = parse_profile(page("codechef_one_contest.html"), "student")
    assert metrics.global_rank is None


# ---------------------------------------------------- the page going wrong


def test_sources_disagreeing_is_a_parse_failure_never_a_guess():
    """If the graph data and the visible text disagree, the page has changed in a
    way we do not understand. Picking one would publish a number we cannot
    defend; refusing makes it PARSE_FAILED, which a developer looks at."""
    tampered = page("codechef_one_contest.html").replace(
        "No. of Contests Participated: <b>1</b>", "No. of Contests Participated: <b>4</b>"
    )
    with pytest.raises(ValueError, match="contest count disagrees"):
        parse_profile(tampered, "student")


def test_missing_rating_data_falls_back_to_the_labels_and_says_so():
    """The embedded JSON disappearing is the most likely redesign. The labels
    still carry the facts, so the scrape survives -- at a lower, recorded tier."""
    without_json = page("codechef_one_contest.html").replace("var all_rating", "var renamed_rating")
    metrics, _, tier = parse_profile(without_json, "student")

    assert tier is ExtractionTier.SEMANTIC_ANCHOR
    assert metrics.contests_attended == 1
    assert metrics.max_rating == 776
    assert metrics.solved_total == 27


def test_never_rated_account_has_no_rating_but_a_measured_zero_contests():
    unrated = page("codechef_one_contest.html")
    start = unrated.index("var all_rating = ")
    end = unrated.index(";", start)
    unrated = unrated[:start] + "var all_rating = []" + unrated[end:]
    unrated = unrated.replace("No. of Contests Participated: <b>1</b>", "No. of Contests Participated: <b>0</b>")
    unrated = unrated.replace("(Highest Rating 776)", "")
    unrated = unrated.replace('class="rating-number">', 'class="rating-number-x">')

    metrics, _, _ = parse_profile(unrated, "student")
    assert metrics.contests_attended == 0      # measured: we asked, the answer is none
    assert metrics.rating is None              # absent: never rated, so no rating
    assert metrics.max_rating is None
    assert metrics.stars is None


def test_a_page_that_is_not_a_profile_is_rejected():
    with pytest.raises(ValueError):
        parse_profile("<html><body>Welcome to CodeChef</body></html>", "x")


# --------------------------------------------------------------- transport


@pytest.mark.asyncio
async def test_redirect_to_home_means_the_handle_does_not_exist():
    """Observed live 2026-10-15: CodeChef answers an unknown handle with a 302 to
    its home page, not a 404."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"location": "https://www.codechef.com/"})

    async with _client(handler) as c:
        result = await CodeChefAdapter(c).scrape("zz_no_such_user")

    assert result.status is SnapshotStatus.HANDLE_NOT_FOUND
    assert not result.status.is_retryable


@pytest.mark.asyncio
async def test_canonicalising_redirect_to_the_same_profile_is_followed():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if request.url.path == "/users/Student":
            return httpx.Response(301, headers={"location": "https://www.codechef.com/users/student"})
        return httpx.Response(200, text=page("codechef_one_contest.html"))

    async with _client(handler) as c:
        result = await CodeChefAdapter(c).scrape("Student")

    assert result.status is SnapshotStatus.OK
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_cloudflare_challenge_is_a_block_not_a_parse_failure():
    """A challenge page must trip the breaker and be retried later. Reading it as
    a parse failure would be terminal and send the student to TnP review for
    something that is the platform's doing, not theirs or ours."""
    challenge = "<html><head><title>Just a moment...</title></head><body>checking</body></html>"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403, text=challenge, headers={"server": "cloudflare", "cf-mitigated": "challenge"}
        )

    async with _client(handler) as c:
        result = await CodeChefAdapter(c).scrape("student")

    assert result.status is SnapshotStatus.RATE_LIMITED
    assert result.status.is_retryable


@pytest.mark.asyncio
async def test_challenge_served_with_200_is_still_a_block():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<title>Just a moment...</title>")

    async with _client(handler) as c:
        result = await CodeChefAdapter(c).scrape("student")

    assert result.status is SnapshotStatus.RATE_LIMITED


@pytest.mark.asyncio
async def test_every_request_waits_for_the_rate_gate():
    waited = []

    async def throttle():
        waited.append(1)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=page("codechef_one_contest.html"))

    async with _client(handler) as c:
        await CodeChefAdapter(c, throttle).scrape("student")

    assert waited == [1]
