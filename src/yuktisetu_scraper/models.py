"""The shapes that cross this service's boundaries."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class Platform(str, Enum):
    """Mirrors org.yuktisetu.profile.model.CodingPlatform exactly.

    The two must not drift: the Java side writes these strings into
    coding_scrape_jobs.platform and a CHECK constraint enforces the set, so a
    value added on one side and not the other fails at insert time rather than
    being silently ignored.
    """

    LEETCODE = "LEETCODE"
    CODEFORCES = "CODEFORCES"
    ATCODER = "ATCODER"
    CODECHEF = "CODECHEF"
    GEEKSFORGEEKS = "GEEKSFORGEEKS"
    HACKERRANK = "HACKERRANK"
    HACKEREARTH = "HACKEREARTH"
    OTHER = "OTHER"


class SnapshotStatus(str, Enum):
    """Why a scrape ended the way it did.

    The distinction between these is the whole value of the field. HANDLE_NOT_FOUND
    is the student's problem and they can fix it; PLATFORM_ERROR is the platform's
    and nobody can; PARSE_FAILED is OURS and means an adapter needs attention. Only
    PLATFORM_ERROR and RATE_LIMITED are worth retrying, and only those should ever
    reach the TnP as a stale-data hold.
    """

    OK = "OK"
    HANDLE_NOT_FOUND = "HANDLE_NOT_FOUND"
    PLATFORM_ERROR = "PLATFORM_ERROR"
    PARSE_FAILED = "PARSE_FAILED"
    RATE_LIMITED = "RATE_LIMITED"

    @property
    def is_retryable(self) -> bool:
        return self in (SnapshotStatus.PLATFORM_ERROR, SnapshotStatus.RATE_LIMITED)


class ExtractionTier(str, Enum):
    """Which rung of the extraction ladder produced the data.

    Recorded per snapshot so a platform silently degrading from its JSON API to a
    regex fallback is visible in the data rather than only in a log nobody reads.
    """

    OFFICIAL_API = "OFFICIAL_API"
    STRUCTURED_API = "STRUCTURED_API"
    EMBEDDED_JSON = "EMBEDDED_JSON"
    SEMANTIC_ANCHOR = "SEMANTIC_ANCHOR"
    REGEX = "REGEX"
    FINGERPRINT = "FINGERPRINT"
    HEADLESS = "HEADLESS"


class Metrics(BaseModel):
    """What the scoring engine reads, normalised across platforms.

    EVERY field is optional, and that is the single most important property here.
    A metric that could not be extracted must be None, never 0: a zero flows into
    a percentile and quietly destroys a real student's rank, while a None is
    excludable and visible. Nothing in this class may ever grow a numeric default.

    Not every platform fills every field, and that is expected -- the per-platform
    percentile is what makes incomparable raw figures comparable, so a platform
    contributing three fields is still useful.
    """

    rating: int | None = None
    max_rating: int | None = None

    contests_attended: int | None = None
    """Contest count, which separates someone who practises from someone who competes."""

    solved_total: int | None = None
    solved_easy: int | None = None
    solved_medium: int | None = None
    solved_hard: int | None = None

    global_rank: int | None = None
    top_percent: float | None = None
    """The platform's OWN percentile, where it publishes one. Not the pool percentile."""

    stars: int | None = None
    """CodeChef's star rating. Null everywhere else."""

    languages: list[str] = Field(default_factory=list)
    """Distinct languages seen. Feeds the 'approach spread' part of the strength scalar."""

    topics: dict[str, int] = Field(default_factory=dict)
    """Tag -> solved count, where the platform exposes it."""

    def is_empty(self) -> bool:
        """True when nothing usable came back.

        A snapshot whose status is OK but whose metrics are empty is a parser that
        silently matched nothing, which is worse than an error because it looks
        like success. The worker treats this as PARSE_FAILED.
        """
        return (
            self.rating is None
            and self.max_rating is None
            and self.solved_total is None
            and self.contests_attended is None
            and self.global_rank is None
            and self.stars is None
            and not self.languages
            and not self.topics
        )


class ScrapeResult(BaseModel):
    """The promise this service always keeps.

    Every scrape returns one of these. There is no path where a caller is left
    with nothing -- a platform being down produces a ScrapeResult with a
    PLATFORM_ERROR status and an error message, not an exception that escapes and
    not a timeout the caller has to interpret. That contract is what lets the
    deadline pipeline decide between "go on to the next platform" and "hold for
    TnP review" without guessing.
    """

    platform: Platform
    handle: str
    status: SnapshotStatus
    tier: ExtractionTier | None = None
    metrics: Metrics = Field(default_factory=Metrics)
    raw: dict[str, Any] | None = None
    error: str | None = None
    scraped_at: datetime

    @property
    def ok(self) -> bool:
        return self.status is SnapshotStatus.OK
