"""SOD durability: no day is ever silently lost.

WHY THIS EXISTS
---------------
LCBO publishes Sale-of-Data as weekday-named files on a rolling seven-day
window: alldlyinventoryMON.zip, alldlyinventoryTUE.zip, and so on. Each weekday
file is OVERWRITTEN seven days later. A day we fail to ingest today is still
sitting on the server for about six more days, and after that it is gone for
good — LCBO keeps no archive we can reach.

The original sync only ever accepted the freshest acceptable file, so a missed
day was never picked up again even while the file was still available. On the NB
tracker that silently lost 2026-07-23, 2026-07-24, 2026-08-01 and 2026-08-08.
Every listing that opened and closed inside a lost day is invisible forever, and
an invisible listing is an unbilled listing.

WHAT THIS MODULE GUARANTEES
---------------------------
1. COVERAGE  Every (source, snapshot_date) actually ingested is recorded
             write-once, so a gap is a visible fact instead of an inference
             from "latest snapshot age".
2. BACKFILL  A gap inside the recoverable window is fetched BY NAME and
             ingested under its own historical date, not today's.
3. ARCHIVE   The tracked-SKU rows for each day are kept compressed and
             write-once, so a day can be replayed even after the file is gone
             and even if the parser or the tracked-SKU list changes later.
4. MEMORY    A gap that ages out of the window is recorded permanently as
             unrecoverable. We would rather carry a known hole than pretend
             the history is complete.

The tables here are append-only. Nothing in this module deletes or rewrites an
ingested day.
"""

import datetime
import hashlib
import json
import zlib

# A file published on day P is overwritten on P+7. Snapshot D is published on
# D+1, so D is reachable until D+8. Six days is the safe working margin.
RECOVERABLE_DAYS = 6

# Client trackers must be able to answer "what did this SKU do, day by day,
# from any date to today" for at least a full year at any moment. Nothing in
# this codebase may prune tracked-SKU SOD rows inside this window. 400 days
# rather than 365 so a year-ago comparison still has a day either side.
RETENTION_MIN_DAYS = 400

_WEEKDAYS = ('MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN')


# ─────────────────────────── file naming ───────────────────────────

def publish_date_for(snapshot_date):
    """The calendar date LCBO publishes a given snapshot date's file.

    Verified against production: alldlyinventoryMON.zip (published Mon
    2026-08-10) carried snapshot 2026-08-09; alldlyinventorySAT.zip
    (published Sat 2026-08-08) carried snapshot 2026-08-07.
    """
    return _as_date(snapshot_date) + datetime.timedelta(days=1)


def weekday_token_for(snapshot_date):
    """MON/TUE/... token of the file that carries this snapshot date."""
    return _WEEKDAYS[publish_date_for(snapshot_date).weekday()]


def filename_for(source, snapshot_date, agent_id=''):
    """Exact SOD filename holding a given snapshot date."""
    wd = weekday_token_for(snapshot_date)
    if source == 'daily_a':
        return f'alldlyinventory{wd}.zip'
    if source == 'daily_b':
        return f'Edlyinventory{agent_id}{wd}.zip'
    raise ValueError(f'Unknown source {source!r}')


def is_recoverable(snapshot_date, today):
    """True while the file carrying this snapshot is still on the server."""
    delta = (_as_date(today) - _as_date(snapshot_date)).days
    return 0 <= delta <= RECOVERABLE_DAYS


def _as_date(d):
    if isinstance(d, datetime.datetime):
        return d.date()
    if isinstance(d, datetime.date):
        return d
    return datetime.datetime.strptime(str(d)[:10], '%Y-%m-%d').date()


# ─────────────────────────── schema ───────────────────────────

def ensure_tables(conn, use_postgres):
    """Create the durability tables. Safe to call on every boot."""
    cur = conn.cursor()
    blob = 'BYTEA' if use_postgres else 'BLOB'
    ts = 'TIMESTAMP' if use_postgres else 'TEXT'
    # SQLite requires a non-constant DEFAULT to be parenthesised.
    now = 'NOW()' if use_postgres else "(datetime('now'))"
    pk = 'SERIAL PRIMARY KEY' if use_postgres else 'INTEGER PRIMARY KEY AUTOINCREMENT'

    # One write-once row per day we actually hold. This is the coverage record
    # AND the replay archive: tracked rows are kept compressed so the day
    # survives the file rolling over.
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS sod_day_archive (
            id              {pk},
            source          TEXT NOT NULL,
            snapshot_date   TEXT NOT NULL,
            file_name       TEXT,
            file_sha256     TEXT,
            file_bytes      INTEGER,
            total_rows      INTEGER,
            tracked_rows    INTEGER,
            rows_gz         {blob},
            ingested_at     {ts} DEFAULT {now},
            ingest_mode     TEXT DEFAULT 'live',
            UNIQUE (source, snapshot_date)
        )
    """)

    # A gap that aged out of the recoverable window. Recorded once, kept
    # forever, never silently dropped.
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS sod_permanent_gaps (
            id              {pk},
            source          TEXT NOT NULL,
            snapshot_date   TEXT NOT NULL,
            detected_at     {ts} DEFAULT {now},
            note            TEXT,
            UNIQUE (source, snapshot_date)
        )
    """)

    # Every backfill attempt, successful or not. An audit trail for the day a
    # client asks why a listing appeared late.
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS sod_backfill_attempts (
            id              {pk},
            source          TEXT NOT NULL,
            snapshot_date   TEXT NOT NULL,
            file_name       TEXT,
            attempted_at    {ts} DEFAULT {now},
            outcome         TEXT,
            detail          TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sod_day_archive_date "
                "ON sod_day_archive (snapshot_date)")
    conn.commit()
    cur.close()


# ─────────────────────────── recording ───────────────────────────

def record_day(conn, use_postgres, source, snapshot_date, file_name='',
               file_sha256='', file_bytes=0, total_rows=0, tracked_rows_data=None,
               ingest_mode='live'):
    """Write-once record of an ingested day, with its tracked rows archived.

    Re-ingesting the same day never overwrites the original record: the first
    capture of a day is the one we keep, so a later feed correction can never
    quietly rewrite history we may already have billed against.
    """
    ph = '%s' if use_postgres else '?'
    rows = tracked_rows_data or []
    payload = None
    if rows:
        payload = zlib.compress(json.dumps(rows, default=str).encode('utf-8'), 6)
        if not use_postgres:
            payload = memoryview(payload).tobytes()

    conflict = ('ON CONFLICT (source, snapshot_date) DO NOTHING'
                if use_postgres else '')
    verb = 'INSERT OR IGNORE INTO' if not use_postgres else 'INSERT INTO'
    cur = conn.cursor()
    cur.execute(
        f"{verb} sod_day_archive (source, snapshot_date, file_name, file_sha256, "
        f"file_bytes, total_rows, tracked_rows, rows_gz, ingest_mode) "
        f"VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph}) {conflict}",
        (source, str(snapshot_date)[:10], file_name, file_sha256, int(file_bytes or 0),
         int(total_rows or 0), len(rows), payload, ingest_mode),
    )
    conn.commit()
    cur.close()


def record_backfill_attempt(conn, use_postgres, source, snapshot_date,
                            file_name, outcome, detail=''):
    ph = '%s' if use_postgres else '?'
    cur = conn.cursor()
    cur.execute(
        f"INSERT INTO sod_backfill_attempts (source, snapshot_date, file_name, "
        f"outcome, detail) VALUES ({ph},{ph},{ph},{ph},{ph})",
        (source, str(snapshot_date)[:10], file_name, outcome, str(detail)[:500]),
    )
    conn.commit()
    cur.close()


def record_permanent_gap(conn, use_postgres, source, snapshot_date, note=''):
    ph = '%s' if use_postgres else '?'
    conflict = ('ON CONFLICT (source, snapshot_date) DO NOTHING'
                if use_postgres else '')
    verb = 'INSERT OR IGNORE INTO' if not use_postgres else 'INSERT INTO'
    cur = conn.cursor()
    cur.execute(
        f"{verb} sod_permanent_gaps (source, snapshot_date, note) "
        f"VALUES ({ph},{ph},{ph}) {conflict}",
        (source, str(snapshot_date)[:10], note),
    )
    conn.commit()
    cur.close()


def sha256_of(b):
    return hashlib.sha256(b).hexdigest() if b else ''


# ─────────────────────────── gap analysis ───────────────────────────

def covered_days(conn, use_postgres, source=None):
    """Snapshot dates we actually hold, from the inventory table itself.

    Deliberately reads sod_inventory rather than only the archive, so that days
    ingested before this module existed still count as covered.
    """
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT snapshot_date FROM sod_inventory "
                "ORDER BY snapshot_date ASC")
    days = {str(r[0])[:10] for r in cur.fetchall() if r and r[0]}
    cur.close()
    return days


def analyze_gaps(conn, use_postgres, today, source='daily_a', earliest=None):
    """Return the coverage picture: held days, recoverable gaps, permanent gaps.

    A gap is any calendar date between the first day we hold and yesterday that
    has no rows. Today is never a gap: LCBO publishes a day's data the morning
    after, so today's snapshot does not exist yet.
    """
    today = _as_date(today)
    held = covered_days(conn, use_postgres, source)
    if not held:
        return {'held': [], 'recoverable': [], 'permanent': [],
                'first_day': None, 'last_day': None}

    first = _as_date(earliest) if earliest else _as_date(min(held))
    last = today - datetime.timedelta(days=1)

    recoverable, permanent = [], []
    d = first
    while d <= last:
        if d.isoformat() not in held:
            (recoverable if is_recoverable(d, today) else permanent).append(d.isoformat())
        d += datetime.timedelta(days=1)

    return {
        'held': sorted(held),
        'recoverable': recoverable,
        'permanent': permanent,
        'first_day': first.isoformat(),
        'last_day': last.isoformat(),
    }


def daily_series(conn, use_postgres, tracked_skus, start, end):
    """Per-day, per-SKU rollup for the tracked SKUs between two dates.

    This is the "day to YTD" view a client asks for: for every day in the
    window, how many stores carried the SKU, how many were actively listed,
    and how many units sat on shelves. Days with no row are returned with
    present=False rather than skipped, so a hole in the history is visible in
    the series instead of silently closing up.
    """
    start, end = _as_date(start), _as_date(end)
    if not tracked_skus:
        return {'days': [], 'skus': []}
    ph = '%s' if use_postgres else '?'
    skus = list(tracked_skus)
    phs = ','.join([ph] * len(skus))
    cur = conn.cursor()
    cur.execute(
        f"SELECT snapshot_date, sku, COUNT(*), "
        f"       SUM(CASE WHEN status='L' THEN 1 ELSE 0 END), "
        f"       SUM(COALESCE(on_hand,0)) "
        f"FROM sod_inventory "
        f"WHERE sku IN ({phs}) AND snapshot_date >= {ph} AND snapshot_date <= {ph} "
        f"GROUP BY snapshot_date, sku ORDER BY snapshot_date ASC",
        tuple(skus) + (start.isoformat(), end.isoformat()),
    )
    by_day = {}
    for d, sku, stores, listed, units in cur.fetchall():
        by_day.setdefault(str(d)[:10], {})[sku] = {
            'stores': int(stores or 0),
            'listed_stores': int(listed or 0),
            'units': int(units or 0),
        }
    cur.close()

    days, d = [], start
    while d <= end:
        key = d.isoformat()
        per_sku = by_day.get(key)
        days.append({
            'date': key,
            'present': per_sku is not None,
            'per_sku': per_sku or {},
            'listed_stores': sum(v['listed_stores'] for v in (per_sku or {}).values()),
            'units': sum(v['units'] for v in (per_sku or {}).values()),
        })
        d += datetime.timedelta(days=1)
    return {'days': days, 'skus': skus}
