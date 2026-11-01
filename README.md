# yuktisetu-scraper

Reads the public coding-platform profiles students submit, and writes one
normalised snapshot per student per platform for YuktiSetu's composite scoring
engine.

Ninth sibling repo under `/Users/hardik/YuktiSetu`, and the only non-Java
component. It is a separate service on purpose: the resilient-scraping ecosystem
is Python, and scraping fails in ways — timeouts, 403s, CAPTCHAs, IP blocks, hung
browsers — that must not share a JVM with `/apply` at an application deadline.

## Why this exists

The composite score needs coding-platform data that cannot be obtained by API
partnership: those talks either failed or went unanswered. Students already give
the TnP a public profile link so it can be verified by eye, so this service reads
the same page, with the same information, automatically.

## Data ownership

| Table | Writer | Readers |
|---|---|---|
| `coding_profile_snapshots` | **only this service**, append-only | the Java fleet |
| `coding_scrape_jobs` | Java enqueues (inside the apply transaction); this service claims and finishes | both |
| `scrape_platform_state` | **only this service** (circuit breakers) | `/health`, the TnP |
| `outbox_events` | this service inserts `CodingScrapeCompleted` rows | drive-service's relay publishes them |

Nothing else in the shared `yuktisetu` schema is touched. This service never
talks to Kafka: it writes its event into the shared outbox in the same
transaction that finishes the job, and drive-service's relay publishes it.

## Platforms

| Platform | Mechanism | Fragility |
|---|---|---|
| Codeforces | official public REST API, one call per 2 s | none |
| LeetCode | public GraphQL, undocumented but structured | low |
| CodeChef | public profile HTML: embedded rating JSON cross-checked against the visible labels | medium |
| GeeksforGeeks, HackerRank, HackerEarth, AtCoder | not built | -- |

CodeChef's terms of service prohibit scraping. Reading the public profile a
student submitted is an accepted decision while access is negotiated
(2026-10-15). Nothing here evades a block: a Cloudflare challenge is reported as
`RATE_LIMITED`, and the platform's lane pauses.

## How a capture runs

1. **The student applies.** drive-service inserts one `coding_scrape_jobs` row
   per platform, inside the apply transaction, with `give_up_at` = 70 minutes
   out. A trigger `NOTIFY`s on commit.
2. **The platform's lane wakes** (one asyncio task per platform), claims the job,
   waits for its rate gate, and scrapes. A successful snapshot under 30 minutes
   old is reused instead.
3. **The result is written in one transaction:** an appended snapshot, the job's
   new state, and, if the job is over, a `CodingScrapeCompleted` outbox row.
4. **Retries**, for platform problems only: at +15 and +60 minutes from queueing,
   then the job FAILS. `HANDLE_NOT_FOUND` and `PARSE_FAILED` fail at once,
   because retrying cannot fix them. A failed job means the student is flagged
   for TnP review.

## Why nothing here can stall a drive

- **Every job has an absolute end.** `give_up_at` cannot be extended by retries,
  pauses or restarts. A pending job past it is failed, with its event, on the
  lane's next wake.
- **Lanes sleep until exactly the next thing they owe**: a `NOTIFY`, a retry
  coming due, a job reaching its limit, or a pause ending. They never poll on a
  fixed interval to make progress. A 60-second idle cap only covers a dropped
  `LISTEN` connection, which reconnects with backoff and then wakes every lane.
- **Circuit breaker per platform.** The first block (429, 403, a challenge page,
  "call limit exceeded"), or three platform errors in a row, pauses that
  platform for 5, 10, 20 and then 40 minutes. While paused, its jobs keep their
  attempts. One probe request decides whether it resumes. The state is
  persisted, so a restart does not forget a block.
- **Every scrape has a hard timeout** (45 s), and no database connection is held
  across a network call. The worker uses an async pool, so a slow query cannot
  freeze the event loop.
- **Lanes are independent.** A CodeChef backlog never delays LeetCode.

## Two rules that matter more than they look

**A value that could not be extracted is `None`, never `0`.** A zero flows into a
percentile and quietly destroys a real student's rank; a `None` is excludable and
visible. The distinction is subtle in one place and tested explicitly: a
Codeforces account that has never been rated has **no rating** (absence → `None`)
and **zero contests** (a measured fact → `0`).

**Snapshots are append-only.** Every attempt, failed or not, is its own row, and
an application points at the exact row it was captured with. Updating one row
per student would let a later application's scrape overwrite the data an earlier
drive must be scored on. The last good snapshot is simply the newest `OK` row,
and an append-only table cannot lose it.

## The promise contract

Every scrape returns a `ScrapeResult`. There is no path where a caller gets an
exception or nothing at all. Statuses, and which are worth retrying:

| Status | Whose problem | Retryable |
|---|---|---|
| `OK` | — | — |
| `HANDLE_NOT_FOUND` | the student's, and they can fix it | no |
| `PARSE_FAILED` | **ours** — an adapter needs attention | no |
| `PLATFORM_ERROR` | the platform's | yes |
| `RATE_LIMITED` | ours, for asking too fast | yes |

Only the retryable two should ever reach the TnP as a stale-data hold. Retrying a
handle that does not exist, or a parser that is broken, just burns rate budget.

`GET /statuses` serves this table so the Java pipeline can assert against it
rather than hardcoding a duplicate that silently diverges.

## Scrape points

**Apply time only** (decided 2026-10-15). The capture taken when the student
applies is what that drive scores them on, and it is frozen: there is no
re-capture, and nothing is scraped at the deadline. Profile-save verification is
the other agreed point; it is not wired yet.

## Running

```bash
./run-local.sh                                  # API + worker, port 8090
.venv/bin/python -m yuktisetu_scraper.canary    # live pre-flight check
.venv/bin/python -m pytest                      # golden fixtures, no network
```

### Endpoints

| | |
|---|---|
| `POST /refresh` | queue a scrape by hand; returns at once |
| `GET /snapshot?student_user_id=&platform=` | the newest snapshot, or a 404 |
| `GET /health` | per lane: breaker state and pause end, request counters (last minute, last hour, since start), average latency, queue depth, current job, last cycle time; plus the LISTEN connection |
| `GET /statuses` | the status vocabulary and retryability |

`/health` reports queue depth and `last_cycle_at` per lane because a lane that
has silently stopped looks exactly like an empty queue from outside. PENDING
climbing while `last_cycle_at` stays old is the signal.

## Canaries

`canary.py` scrapes a few long-established real accounts and asserts loose floors
on them. Run it before any batch, and nightly.

This is the most valuable control in the service. A silently-broken parser is far
worse than an outage: an outage is loud and recoverable, while a parser returning
plausible nonsense corrupts scores students have already been shown. A canary
failure should abort a batch, not warn.

## Tests

`pytest` runs golden fixtures only — saved real payloads, no network. A failure
there means **our parser broke**, not that a platform is having a bad day. Live
checks are the canary's job and are opt-in.

## Schema

`../infra/migrations/2026-10-06-coding-scrape-tables.sql`, then
`../infra/migrations/2026-10-15-apply-time-coding-capture.sql`. Note the `OWNER TO yuktisetu`
lines: applying the file from a superuser shell account creates tables owned by
that account, the services connect as `yuktisetu`, and the result is "permission
denied for table" — which the Java test suites do not catch, because they run
`ddl-auto=create-drop` and Hibernate creates tables as whichever role connected.
