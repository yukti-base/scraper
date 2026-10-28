"""Settings, read from the environment."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict

from .models import Platform


def _minutes(csv: str) -> list[int]:
    return [int(x) for x in csv.replace(" ", "").split(",") if x]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="YS_", env_file=".env", extra="ignore")

    db_url: str = "postgresql://yuktisetu:YuktiSetu!2026@localhost:5432/yuktisetu"
    log_level: str = "INFO"
    api_port: int = 8090

    api_pool_max: int = 4
    """Connections for the HTTP endpoints, which run in FastAPI's thread pool."""

    worker_pool_max: int = 6
    """Connections for the lanes. One lane holds at most one at a time and only
    for milliseconds -- never across a scrape -- so this is lanes plus headroom."""

    snapshot_fresh_minutes: int = 30
    """Reuse a successful snapshot younger than this instead of scraping again.

    Short on purpose under apply-time capture: the student's figures are meant to
    be what they were when they applied, so this only absorbs a student applying
    to several drives in one sitting, not a whole day of practice."""

    retry_offsets_minutes: str = "15,60"
    """When the 2nd and 3rd attempts happen, measured from when the job was
    queued. 15 then 60 is "retry after 15 minutes, then after 45 more", as agreed.
    A job is never attempted more than len(offsets) + 1 times."""

    breaker_pause_minutes: str = "5,10,20,40"
    """How long a platform is paused after its 1st, 2nd, 3rd, 4th+ consecutive
    block. Capped below the job window, so a paused platform still gets one
    honest try at its queued jobs before they reach give_up_at."""

    breaker_error_threshold: int = 3
    """Consecutive PLATFORM_ERRORs (5xx, timeouts) that also pause a platform.
    Without this a platform that is simply down would burn every queued job's
    attempts in a few seconds instead of waiting for it to recover."""

    scrape_hard_timeout_seconds: float = 45.0
    """Absolute ceiling on one scrape, on top of httpx's own timeouts. A hung
    read must never pin a lane, because a pinned lane looks exactly like an empty
    queue from outside."""

    http_timeout_seconds: float = 20.0

    lane_idle_max_seconds: float = 60.0
    """The longest a lane sleeps with nothing scheduled. Lanes are woken by
    NOTIFY the instant a job is queued; this only bounds the damage if the LISTEN
    connection is down and a notification is missed."""

    # Requests per second, per platform. Biased low on purpose: being blocked
    # cannot be undone quickly, and no single apply is urgent enough to risk it.
    rate_codeforces: float = 0.5
    """Codeforces' published API limit is one call per two seconds."""
    rate_leetcode: float = 1.0
    rate_codechef: float = 0.5
    rate_default: float = 0.5

    blocked_platforms_csv: str = "CODECHEF"
    """Platforms that must NEVER be read (env YS_BLOCKED_PLATFORMS_CSV).

    CodeChef is blocked as of 2026-10-28: its terms of service forbid automated
    access to the site. A blocked platform gets no lane, its adapter refuses to
    run, and any job left for it is closed at startup without a single request.
    Unblocking needs a deliberate change here AND written permission from the
    platform -- not just a code change."""

    @property
    def blocked_platforms(self) -> set[Platform]:
        return {Platform(p.strip()) for p in self.blocked_platforms_csv.split(",") if p.strip()}

    def rate_for(self, platform: Platform) -> float:
        return {
            Platform.CODEFORCES: self.rate_codeforces,
            Platform.LEETCODE: self.rate_leetcode,
            Platform.CODECHEF: self.rate_codechef,
        }.get(platform, self.rate_default)

    @property
    def retry_offsets(self) -> list[int]:
        return _minutes(self.retry_offsets_minutes)

    @property
    def breaker_pauses(self) -> list[int]:
        return _minutes(self.breaker_pause_minutes)


settings = Settings()
