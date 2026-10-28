"""LeetCode, via the public GraphQL endpoint its own front-end calls.

Not officially documented, but a structured contract rather than HTML: the
profile page is a client of https://leetcode.com/graphql, so we call the same
thing it does. That matters for fragility -- a visual redesign cannot break this,
and a field rename breaks one named query rather than a parser.

Two queries in one request, because LeetCode allows it and it halves the rate
budget per student:
  matchedUser       -- solved counts by difficulty, profile ranking, languages
  userContestRanking -- contest rating, attendance, global rank, top percentage

userContestRanking is null for anyone who has never entered a contest, which is
most students. That is not an error.
"""

from __future__ import annotations

from ..models import ExtractionTier, Metrics, Platform
from .base import Adapter, HandleNotFound

ENDPOINT = "https://leetcode.com/graphql"

QUERY = """
query yuktisetuProfile($username: String!) {
  matchedUser(username: $username) {
    username
    profile { ranking reputation }
    submitStatsGlobal { acSubmissionNum { difficulty count } }
    languageProblemCount { languageName problemsSolved }
    tagProblemCounts {
      advanced { tagName problemsSolved }
      intermediate { tagName problemsSolved }
      fundamental { tagName problemsSolved }
    }
  }
  userContestRanking(username: $username) {
    attendedContestsCount
    rating
    globalRanking
    topPercentage
  }
}
"""


class LeetCodeAdapter(Adapter):
    platform = Platform.LEETCODE
    tier = ExtractionTier.STRUCTURED_API

    async def fetch(self, handle: str) -> tuple[Metrics, dict]:
        await self.throttle()
        resp = await self._client.post(
            ENDPOINT,
            json={"query": QUERY, "variables": {"username": handle}},
            headers={
                "Content-Type": "application/json",
                # LeetCode 403s a request with no Referer. This is the one place
                # the adapter has to look like a browser, and it is the single
                # most likely thing to need revisiting if it ever starts failing.
                "Referer": f"https://leetcode.com/u/{handle}/",
            },
        )
        resp.raise_for_status()
        body = resp.json()

        # GraphQL reports a missing user as a 200 with an errors array, so the
        # HTTP status alone never reveals a bad handle.
        if body.get("errors"):
            messages = "; ".join(e.get("message", "") for e in body["errors"])
            if "not exist" in messages.lower() or "not found" in messages.lower():
                raise HandleNotFound(f"leetcode: {messages}")
            raise RuntimeError(f"leetcode graphql errors: {messages}")

        data = body.get("data") or {}
        user = data.get("matchedUser")
        if not user:
            raise HandleNotFound(f"leetcode: no user '{handle}'")

        metrics = Metrics()

        # acSubmissionNum carries an "All" bucket alongside the three
        # difficulties. Keyed by difficulty rather than positionally, because the
        # order is not promised anywhere.
        by_difficulty = {
            (row.get("difficulty") or "").lower(): row.get("count")
            for row in ((user.get("submitStatsGlobal") or {}).get("acSubmissionNum") or [])
        }
        metrics.solved_total = by_difficulty.get("all")
        metrics.solved_easy = by_difficulty.get("easy")
        metrics.solved_medium = by_difficulty.get("medium")
        metrics.solved_hard = by_difficulty.get("hard")

        profile = user.get("profile") or {}
        metrics.global_rank = profile.get("ranking")

        metrics.languages = [
            row["languageName"]
            for row in (user.get("languageProblemCount") or [])
            if row.get("languageName")
        ]

        tags = user.get("tagProblemCounts") or {}
        for bucket in ("advanced", "intermediate", "fundamental"):
            for row in tags.get(bucket) or []:
                name, solved = row.get("tagName"), row.get("problemsSolved")
                if name and solved:
                    metrics.topics[name] = solved

        # Null for the majority of students, who never enter contests. Absent
        # contest data is a real fact about them, not a failure.
        contest = data.get("userContestRanking")
        if contest:
            metrics.contests_attended = contest.get("attendedContestsCount")
            rating = contest.get("rating")
            # LeetCode returns a float here; the column and the scoring scalar
            # both want an int, and sub-point precision is meaningless.
            metrics.rating = int(round(rating)) if rating is not None else None
            metrics.top_percent = contest.get("topPercentage")
            if contest.get("globalRanking") is not None:
                metrics.global_rank = contest["globalRanking"]

        return metrics, {"matchedUser": user, "userContestRanking": contest}
