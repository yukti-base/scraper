"""Synchronous database access, for the HTTP endpoints only.

FastAPI runs plain `def` endpoints in its thread pool, so a blocking driver is
correct here. The worker lanes run on the event loop and must NEVER use this
pool -- a blocking call there freezes every lane and the API at once. They use
the async pool in store.py.

Ownership, which is the rule that makes a shared database safe:
  coding_profile_snapshots -- this service is the ONLY writer. Java reads it.
  coding_scrape_jobs       -- Java enqueues; this service claims and finishes.
  scrape_platform_state    -- this service only.
  outbox_events            -- this service INSERTS its CodingScrapeCompleted
                              rows; drive-service's relay publishes them.
Nothing else in the schema is touched.
"""

from __future__ import annotations

import logging

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .config import settings
from .models import Platform

log = logging.getLogger(__name__)

pool = ConnectionPool(
    settings.db_url,
    min_size=1,
    max_size=settings.api_pool_max,
    kwargs={"row_factory": dict_row},
    open=False,
)


def get_latest_snapshot(conn: Connection, student_user_id: int, platform: Platform) -> dict | None:
    """The newest snapshot of any status -- what the last attempt saw."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, student_user_id, platform, handle, status, scraped_at,
                   extraction_tier, metrics, last_error, job_id
              FROM coding_profile_snapshots
             WHERE student_user_id = %s AND platform = %s
             ORDER BY scraped_at DESC, id DESC
             LIMIT 1
            """,
            (student_user_id, platform.value),
        )
        return cur.fetchone()


def enqueue(
    conn: Connection,
    student_user_id: int,
    platform: Platform,
    handle: str,
    reason: str,
    posting_id: int | None = None,
) -> int | None:
    """Queue a scrape, or return None if one is already queued.

    uk_scrape_job_pending makes the duplicate impossible at the database rather
    than by checking first, so two simultaneous requests cannot both insert.
    give_up_at takes the column default.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO coding_scrape_jobs
                   (student_user_id, platform, handle, reason, posting_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING id
            """,
            (student_user_id, platform.value, handle, reason, posting_id),
        )
        row = cur.fetchone()
    return row["id"] if row else None


def job_counts(conn: Connection) -> dict[str, dict[str, int]]:
    """Queue depth per platform and status, for /health."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT platform, status, count(*) AS n
              FROM coding_scrape_jobs
             WHERE status IN ('PENDING','RUNNING')
                OR finished_at > now() - interval '1 hour'
             GROUP BY platform, status
            """
        )
        out: dict[str, dict[str, int]] = {}
        for r in cur.fetchall():
            out.setdefault(r["platform"], {})[r["status"]] = r["n"]
        return out


def platform_states(conn: Connection) -> dict[str, dict]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT platform, state, paused_until, consecutive_trips, last_reason "
            "FROM scrape_platform_state"
        )
        return {r["platform"]: r for r in cur.fetchall()}
