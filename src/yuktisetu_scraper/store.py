"""Async database access for the worker lanes.

Every function here is a short transaction. None is ever held open across a
scrape: a job is claimed and committed, the network call happens with no
connection held, and the result is written in a second short transaction. That
is what keeps the pool small and stops a slow platform from tying up the
database -- or blocking Java's apply path, which may need the same job row.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from .config import settings
from .models import Platform, ScrapeResult, SnapshotStatus
from .policy import Breaker, BreakerState

log = logging.getLogger(__name__)

COMPLETED_TOPIC = "yuktisetu.coding.scrape.completed.v1"
"""Mirrors DriveTopics.CODING_SCRAPE_COMPLETED in messaging-dal."""

pool = AsyncConnectionPool(
    settings.db_url,
    min_size=1,
    max_size=settings.worker_pool_max,
    kwargs={"row_factory": dict_row},
    open=False,
)


# ----------------------------------------------------------------- claiming


async def claim_one(platform: Platform) -> dict | None:
    """Claim the next due job for one platform, or None.

    One at a time, deliberately. The rate gate allows one request every couple
    of seconds anyway, and claiming a batch would leave jobs marked RUNNING while
    the breaker trips partway through it.
    """
    async with pool.connection() as conn:
        cur = await conn.execute(
            """
            UPDATE coding_scrape_jobs SET
                   status     = 'RUNNING',
                   claimed_at = now(),
                   attempts   = attempts + 1,
                   updated_at = now()
             WHERE id = (
                   SELECT id FROM coding_scrape_jobs
                    WHERE platform = %s AND status = 'PENDING'
                      AND available_at <= now() AND give_up_at > now()
                    ORDER BY available_at, id
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED)
            RETURNING id, student_user_id, platform, handle, reason, posting_id,
                      attempts, created_at, give_up_at
            """,
            (platform.value,),
        )
        return await cur.fetchone()


async def next_wake_at(platform: Platform) -> datetime | None:
    """When this platform's lane next has something to do.

    Either a retry coming due or a pending job reaching its limit -- whichever is
    sooner. The lane sleeps exactly until then, so nothing polls.
    """
    async with pool.connection() as conn:
        cur = await conn.execute(
            """
            SELECT least(min(available_at), min(give_up_at)) AS at
              FROM coding_scrape_jobs
             WHERE platform = %s AND status = 'PENDING'
            """,
            (platform.value,),
        )
        row = await cur.fetchone()
        return row["at"] if row else None


async def next_give_up_at(platform: Platform) -> datetime | None:
    """The soonest time limit among this platform's pending jobs.

    What a PAUSED lane wakes for, besides the pause ending: a job reaching its
    limit must still be failed on time. Jobs that are merely due (available_at)
    are not a reason to wake -- the lane cannot take them while paused, and
    waking for an already-due job is a sleep of zero, i.e. a busy loop.
    """
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT min(give_up_at) AS at FROM coding_scrape_jobs "
            "WHERE platform = %s AND status = 'PENDING'",
            (platform.value,),
        )
        row = await cur.fetchone()
        return row["at"] if row else None


async def close_blocked_jobs(blocked: set[Platform]) -> int:
    """End every unfinished job for a platform we must not read -- with no request.

    Each ends FAILED, with the usual completion event, so drive-service settles
    the capture and no drive ever waits on a platform that will never be read.
    Runs at startup, BEFORE any lane starts (RUNNING jobs are orphans then).
    """
    if not blocked:
        return 0
    async with pool.connection() as conn:
        async with conn.transaction():
            cur = await conn.execute(
                """
                UPDATE coding_scrape_jobs SET
                       status = 'FAILED', finished_at = now(),
                       last_status = 'PLATFORM_ERROR',
                       last_error = 'not read: this platform is switched off (its terms forbid automated access)',
                       updated_at = now()
                 WHERE platform = ANY(%s) AND status IN ('PENDING', 'RUNNING')
                RETURNING id, student_user_id, platform, handle, reason, posting_id,
                          attempts, last_status, last_error
                """,
                ([p.value for p in blocked],),
            )
            jobs = await cur.fetchall()
            for job in jobs:
                await _write_completed_event(
                    conn, job, job_status="FAILED", outcome=job["last_status"],
                    snapshot_id=None, error=job["last_error"],
                )
            return len(jobs)


async def reap_orphans() -> int:
    """Return RUNNING jobs to PENDING at startup.

    This process is the only worker, so anything RUNNING when it starts was left
    by a previous process that died mid-scrape. Their give_up_at still applies,
    so an orphan cannot outlive its bound.
    """
    async with pool.connection() as conn:
        cur = await conn.execute(
            """
            UPDATE coding_scrape_jobs SET
                   status     = 'PENDING',
                   claimed_at = NULL,
                   last_error = 'reclaimed: the previous worker stopped while holding this job',
                   updated_at = now()
             WHERE status = 'RUNNING'
            """
        )
        return cur.rowcount


# --------------------------------------------------------------- snapshots


async def fresh_snapshot_id(student_user_id: int, platform: Platform, handle: str) -> int | None:
    """A successful snapshot of THIS handle young enough to reuse, if there is one.

    Matching the handle matters: a student who corrects their username (a
    re-check, or a profile edit) and applies within the freshness window must be
    read under the new handle, not handed the old account's data.
    """
    async with pool.connection() as conn:
        cur = await conn.execute(
            """
            SELECT id FROM coding_profile_snapshots
             WHERE student_user_id = %s AND platform = %s AND status = 'OK'
               AND lower(handle) = lower(%s)
               AND scraped_at > now() - (interval '1 minute' * %s)
             ORDER BY scraped_at DESC, id DESC
             LIMIT 1
            """,
            (student_user_id, platform.value, handle, settings.snapshot_fresh_minutes),
        )
        row = await cur.fetchone()
        return row["id"] if row else None


async def _insert_snapshot(conn: AsyncConnection, job: dict, result: ScrapeResult) -> int:
    """Append one scrape outcome. Never updates an existing row.

    Failures are recorded as rows too, with no metrics, so the history shows
    every attempt. The last GOOD snapshot is simply the newest OK row, which is
    what the TnP falls back to -- an append-only table cannot lose it.
    """
    cur = await conn.execute(
        """
        INSERT INTO coding_profile_snapshots
               (student_user_id, platform, handle, status, scraped_at,
                extraction_tier, raw, metrics, last_error, job_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            job["student_user_id"],
            result.platform.value,
            result.handle,
            result.status.value,
            result.scraped_at,
            result.tier.value if result.tier else None,
            Jsonb(result.raw) if (result.ok and result.raw is not None) else None,
            Jsonb(json.loads(result.metrics.model_dump_json())) if result.ok else None,
            result.error,
            job["id"],
        ),
    )
    return (await cur.fetchone())["id"]


# ---------------------------------------------------------------- finishing


async def record_attempt(
    job: dict,
    result: ScrapeResult,
    retry_at: datetime | None,
) -> dict:
    """Write one attempt's snapshot and move the job on, in ONE transaction.

    retry_at set  -> back to PENDING, due at retry_at.
    retry_at None -> terminal (DONE on success, FAILED otherwise), and the
                     CodingScrapeCompleted event is written in this same
                     transaction. The job ending and the world hearing about it
                     cannot be separated: a job that finished without its event
                     is a drive that waits for a signal that never comes.
    """
    async with pool.connection() as conn:
        async with conn.transaction():
            snapshot_id = await _insert_snapshot(conn, job, result)

            if retry_at is not None:
                await conn.execute(
                    """
                    UPDATE coding_scrape_jobs SET
                           status = 'PENDING', available_at = %s, claimed_at = NULL,
                           last_status = %s, last_error = %s, updated_at = now()
                     WHERE id = %s
                    """,
                    (retry_at, result.status.value, result.error, job["id"]),
                )
                return {"terminal": False, "snapshot_id": snapshot_id}

            ok = result.status is SnapshotStatus.OK
            await conn.execute(
                """
                UPDATE coding_scrape_jobs SET
                       status = %s, finished_at = now(), last_status = %s,
                       last_error = %s, snapshot_id = %s, updated_at = now()
                 WHERE id = %s
                """,
                (
                    "DONE" if ok else "FAILED",
                    result.status.value,
                    result.error,
                    snapshot_id if ok else None,
                    job["id"],
                ),
            )
            event_id = await _write_completed_event(
                conn,
                job,
                job_status="DONE" if ok else "FAILED",
                outcome=result.status.value,
                snapshot_id=snapshot_id if ok else None,
                error=result.error,
            )
            return {"terminal": True, "snapshot_id": snapshot_id, "event_id": event_id}


async def finish_with_fresh_snapshot(job: dict, snapshot_id: int) -> str:
    """Close a job by reusing a snapshot that is still fresh. Same event contract."""
    async with pool.connection() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                UPDATE coding_scrape_jobs SET
                       status = 'DONE', finished_at = now(), last_status = 'OK',
                       last_error = 'reused a snapshot still fresh', snapshot_id = %s,
                       updated_at = now()
                 WHERE id = %s
                """,
                (snapshot_id, job["id"]),
            )
            return await _write_completed_event(
                conn, job, job_status="DONE", outcome="OK", snapshot_id=snapshot_id, error=None
            )


async def expire_overdue(platform: Platform) -> list[int]:
    """Fail every pending job on this platform that has passed its limit.

    This is the guarantee that the scraper can never stall a drive. A job reaches
    give_up_at only when its platform has kept failing or stayed paused, and at
    that point it ENDS -- with its event -- and the student goes to TnP review
    instead of the drive waiting on a platform that is not coming back tonight.
    """
    async with pool.connection() as conn:
        async with conn.transaction():
            cur = await conn.execute(
                """
                UPDATE coding_scrape_jobs SET
                       status = 'FAILED', finished_at = now(),
                       last_status = coalesce(last_status, 'PLATFORM_ERROR'),
                       last_error = 'gave up: not captured within the job''s time limit'
                                    || coalesce(' (last: ' || last_error || ')', ''),
                       updated_at = now()
                 WHERE platform = %s AND status = 'PENDING' AND give_up_at <= now()
                RETURNING id, student_user_id, platform, handle, reason, posting_id,
                          attempts, last_status, last_error
                """,
                (platform.value,),
            )
            expired = await cur.fetchall()
            for job in expired:
                await _write_completed_event(
                    conn,
                    job,
                    job_status="FAILED",
                    outcome=job["last_status"],
                    snapshot_id=None,
                    error=job["last_error"],
                )
            return [j["id"] for j in expired]


async def _write_completed_event(
    conn: AsyncConnection,
    job: dict,
    *,
    job_status: str,
    outcome: str,
    snapshot_id: int | None,
    error: str | None,
) -> str:
    """Insert a CodingScrapeCompleted row into the shared outbox.

    Shaped exactly like Java's EventEnvelope, so drive-service reads it with the
    same code it reads its own events with, and published by the relay that
    already runs there. This service never talks to Kafka directly -- the outbox
    is what makes the event atomic with the job's final state.
    """
    event_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    envelope = {
        "eventId": event_id,
        "eventType": "CodingScrapeCompleted",
        "version": "v1",
        "occurredAt": now.isoformat(),
        "actorUserId": None,
        "payload": {
            "jobId": job["id"],
            "studentUserId": job["student_user_id"],
            "platform": job["platform"],
            "handle": job["handle"],
            "jobStatus": job_status,
            "outcome": outcome,
            "snapshotId": snapshot_id,
            "attempts": job["attempts"],
            "error": error,
            "finishedAt": now.isoformat(),
        },
    }
    await conn.execute(
        """
        INSERT INTO outbox_events
               (event_id, aggregate_type, aggregate_id, topic, event_type,
                partition_key, payload, status, attempts, available_at,
                created_at, updated_at)
        VALUES (%s, 'CodingScrapeJob', %s, %s, 'CodingScrapeCompleted',
                %s, %s, 'PENDING', 0, now(), now(), now())
        """,
        (
            event_id,
            str(job["id"]),
            COMPLETED_TOPIC,
            str(job["student_user_id"]),
            Jsonb(envelope),
        ),
    )
    log.debug(
        "completion event queued event=%s job=%s status=%s outcome=%s snapshot=%s",
        event_id, job["id"], job_status, outcome, snapshot_id,
    )
    return event_id


# ------------------------------------------------------------ breaker state


async def load_breaker(platform: Platform, breaker: Breaker) -> None:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT state, paused_until, consecutive_trips, last_reason "
            "FROM scrape_platform_state WHERE platform = %s",
            (platform.value,),
        )
        row = await cur.fetchone()
    if row:
        breaker.state = BreakerState(row["state"])
        breaker.paused_until = row["paused_until"]
        breaker.consecutive_trips = row["consecutive_trips"]
        breaker.last_reason = row["last_reason"]


async def save_breaker(platform: Platform, breaker: Breaker) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            """
            INSERT INTO scrape_platform_state
                   (platform, state, paused_until, consecutive_trips,
                    last_tripped_at, last_reason, updated_at)
            VALUES (%(p)s, %(s)s, %(u)s, %(t)s,
                    CASE WHEN %(tripped)s THEN now() END, %(r)s, now())
            ON CONFLICT (platform) DO UPDATE SET
                   state             = EXCLUDED.state,
                   paused_until      = EXCLUDED.paused_until,
                   consecutive_trips = EXCLUDED.consecutive_trips,
                   last_tripped_at   = coalesce(EXCLUDED.last_tripped_at,
                                                scrape_platform_state.last_tripped_at),
                   last_reason       = EXCLUDED.last_reason,
                   updated_at        = now()
            """,
            {
                "p": platform.value,
                "s": breaker.state.value,
                "u": breaker.paused_until,
                "t": breaker.consecutive_trips,
                "r": breaker.last_reason,
                # A separate boolean, not "%(s)s = 'OPEN'": the same placeholder
                # used as the state column AND compared with text made Postgres
                # deduce two types for it, so every trip failed to save.
                "tripped": breaker.state is BreakerState.OPEN,
            },
        )
