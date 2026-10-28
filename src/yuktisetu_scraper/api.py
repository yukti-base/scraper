"""The HTTP surface, and the worker's host process.

Small on purpose. Java does NOT call this to capture an application -- it inserts
the job row inside its own apply transaction, and a database trigger wakes the
worker. That keeps the apply atomic with its capture and keeps this service off
the apply path entirely: if the scraper is down, students still apply, and the
jobs wait in the table.

What is left here is manual enqueueing, a read of the latest snapshot, and
health.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from . import db, store
from .config import settings
from .logging_setup import configure as configure_logging
from .models import Platform, SnapshotStatus
from .platforms import ADAPTERS
from .worker import Worker

configure_logging()
log = logging.getLogger(__name__)

_worker: Worker | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _worker
    db.pool.open(wait=True, timeout=10)
    await store.pool.open(wait=True, timeout=10)
    log.info("database pools open: %s", settings.db_url.rsplit("@", 1)[-1])

    _worker = Worker()
    await _worker.start()
    try:
        yield
    finally:
        log.info("shutting down")
        await _worker.stop()
        await store.pool.close()
        db.pool.close()


app = FastAPI(title="YuktiSetu Scraper", version="0.2.0", lifespan=lifespan)


class EnqueueRequest(BaseModel):
    student_user_id: int
    platform: Platform
    handle: str
    reason: str = "MANUAL"
    posting_id: int | None = None


@app.post("/refresh")
def refresh(req: EnqueueRequest) -> dict:
    """Queue a scrape by hand. Returns immediately; a lane does the work."""
    if req.platform in settings.blocked_platforms:
        raise HTTPException(status_code=400,
                            detail=f"{req.platform.value} is blocked: its terms forbid automated access")
    if req.platform not in ADAPTERS:
        raise HTTPException(status_code=400, detail=f"no adapter for {req.platform.value}")
    with db.pool.connection() as conn:
        job_id = db.enqueue(
            conn, req.student_user_id, req.platform, req.handle, req.reason, req.posting_id
        )
        conn.commit()
    if job_id is None:
        return {"queued": False, "reason": "a job for this student and platform is already pending"}
    return {"queued": True, "job_id": job_id}


@app.get("/snapshot")
def snapshot(student_user_id: int, platform: Platform) -> dict:
    """The newest snapshot for a student on a platform, or a definite 404."""
    with db.pool.connection() as conn:
        row = db.get_latest_snapshot(conn, student_user_id, platform)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"no snapshot for student {student_user_id} on {platform.value}",
        )
    return row


@app.get("/health")
def health() -> dict:
    """What is actually true, per platform.

    Queue depth is here because a lane that has silently stopped looks exactly
    like an empty queue from outside: PENDING climbing while last_cycle_at stays
    old is the signal. The breaker state says whether a platform is paused and
    until when, and the request counters say what the platform has been
    answering.
    """
    jobs: dict = {}
    db_ok = True
    try:
        with db.pool.connection() as conn:
            jobs = db.job_counts(conn)
    except Exception as e:  # noqa: BLE001
        db_ok = False
        log.error("health check could not reach the database: %s", e)

    lanes = {}
    if _worker:
        for platform, lane in _worker.lanes.items():
            lanes[platform.value] = lane.health() | {"jobs": jobs.get(platform.value, {})}

    degraded = not db_ok or not (_worker and _worker.listener_connected)
    return {
        "status": "degraded" if degraded else "up",
        "database": "ok" if db_ok else "unreachable",
        "listener": "connected" if (_worker and _worker.listener_connected) else "disconnected",
        "lanes": lanes,
        "blocked_platforms": sorted(p.value for p in settings.blocked_platforms),
        "platforms_without_adapter": sorted(
            p.value for p in Platform if p not in ADAPTERS and p is not Platform.OTHER
        ),
    }


@app.get("/statuses")
def statuses() -> dict:
    """The status vocabulary and which values are worth retrying."""
    return {s.value: {"retryable": s.is_retryable} for s in SnapshotStatus}
