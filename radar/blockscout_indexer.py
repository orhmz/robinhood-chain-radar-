"""Durable, fail-closed ingestion of Blockscout contract-creation pages.

This is a source index, not proof of complete chain history or CREATE2 coverage.
It never sends notifications or advances the existing RPC discovery checkpoint.
"""

import argparse
from contextlib import closing
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import time

from radar.blockscout_probe import SourceError, fetch


CHAINS = (4663, 5042)
UTC = dt.timezone.utc
TRANSIENT_HTTP = ("HTTP 429", "HTTP 500", "HTTP 502", "HTTP 503", "HTTP 504")
TRANSIENT_TRANSPORT = ("transport or JSON error (TimeoutError)",
                       "transport or JSON error (URLError)")


def parse_time(value):
    if not isinstance(value, str):
        raise SourceError("Missing or invalid UTC timestamp")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise SourceError("Missing or invalid UTC timestamp") from None
    if parsed.tzinfo is None:
        raise SourceError("Timestamp has no timezone")
    return parsed.astimezone(UTC)


def stamp(value):
    if value.tzinfo is None:
        raise ValueError("UTC timestamp required")
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def hex_value(value, length, label):
    if not isinstance(value, str) or len(value) != length or not value.startswith("0x"):
        raise SourceError(f"Invalid {label}")
    try:
        int(value[2:], 16)
    except ValueError:
        raise SourceError(f"Invalid {label}") from None
    return value.lower()


def creation(item, start, end):
    if not isinstance(item, dict) or item.get("type") != "contract_creation":
        raise SourceError("Creation filter returned an unexpected item")
    status = item.get("status")
    created = item.get("created_contract")
    if isinstance(status, str) and (
            status in ("error", "failed", "out of gas") or
            status.startswith("execution reverted")):
        return None  # A reverted attempt is not a deployment, even with an address.
    if status != "success":
        raise SourceError(f"Unrecognized creation status: {status!r}")
    if not isinstance(created, dict):
        raise SourceError("Creation item lacks a created contract")
    transaction = hex_value(item.get("hash"), 66, "transaction hash")
    address = hex_value(created.get("hash"), 42, "created contract address")
    block = item.get("block_number")
    if type(block) is not int or block < 0:
        raise SourceError("Creation item lacks a valid block number")
    internal = item.get("internal_transaction_index")
    if internal is not None and (type(internal) is not int or internal < 0):
        raise SourceError("Invalid internal transaction index")
    observed = parse_time(item.get("timestamp"))
    if not start <= observed <= end:
        raise SourceError("Creation item timestamp falls outside requested interval")
    return (transaction, -1 if internal is None else internal, address, block, stamp(observed))


class Client:
    def __init__(self, key, fetcher=fetch, *, min_interval=0.25, sleeper=time.sleep,
                 clock=time.monotonic):
        if not key:
            raise SourceError("BLOCKSCOUT_API_KEY is missing")
        self.key = key
        self.fetcher = fetcher
        self.min_interval = min_interval
        self.sleeper = sleeper
        self.clock = clock
        self.last_call = None

    def get(self, chain_id, params):
        for attempt in range(4):
            if self.last_call is not None:
                delay = self.min_interval - (self.clock() - self.last_call)
                if delay > 0:
                    self.sleeper(delay)
            self.last_call = self.clock()
            try:
                return self.fetcher(chain_id, "advanced-filters", params, self.key)
            except SourceError as error:
                if attempt == 3 or not any(
                        code in str(error) for code in TRANSIENT_HTTP + TRANSIENT_TRANSPORT):
                    raise
                self.sleeper(2 ** attempt)
        raise AssertionError("unreachable")


def read_window(client, chain_id, start, end, *, max_pages=1000):
    """Exhaust one closed interval. Adjacent intervals share a boundary and dedupe."""
    if max_pages < 1 or not start < end:
        raise ValueError("Invalid window or page limit")
    base = {"transaction_types": "CONTRACT_CREATION",
            "age_from": stamp(start), "age_to": stamp(end)}
    params = base.copy()
    cursors = set()
    rows = {}
    pages = 0
    while True:
        payload = client.get(chain_id, params)
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), list) or (
                "next_page_params" not in payload):
            raise SourceError("Incomplete advanced-filter page")
        pages += 1
        for item in payload["items"]:
            row = creation(item, start, end)
            if row is None:
                continue
            identity = row[:3]
            if identity in rows:
                raise SourceError("Duplicate creation within one interval")
            rows[identity] = row
        cursor = payload["next_page_params"]
        if cursor is None:
            break
        if not payload["items"] or not isinstance(cursor, dict) or not cursor:
            raise SourceError("Empty or malformed paginated creation page")
        if any(key in base for key in cursor):
            raise SourceError("Pagination cursor overrides interval filters")
        signature = json.dumps(cursor, sort_keys=True, separators=(",", ":"))
        if signature in cursors:
            raise SourceError("Repeated pagination cursor")
        cursors.add(signature)
        if pages >= max_pages:
            raise SourceError("Page budget exhausted before interval completion")
        params = {**base, **cursor}
    ordered = sorted(rows.values())
    return ordered, pages, rows_digest(ordered)


def rows_digest(rows):
    return hashlib.sha256(json.dumps(rows, separators=(",", ":")).encode()).hexdigest()


def open_store(path):
    db = sqlite3.connect(path)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS streams (
            chain_id INTEGER PRIMARY KEY, bootstrap_utc TEXT NOT NULL,
            next_start_utc TEXT NOT NULL, window_seconds INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS source_windows (
            chain_id INTEGER NOT NULL, start_utc TEXT NOT NULL, end_utc TEXT NOT NULL,
            page_count INTEGER NOT NULL, creation_count INTEGER NOT NULL,
            digest TEXT NOT NULL, checked_at_utc TEXT NOT NULL,
            PRIMARY KEY (chain_id, start_utc));
        CREATE TABLE IF NOT EXISTS source_creations (
            chain_id INTEGER NOT NULL, transaction_hash TEXT NOT NULL,
            internal_index INTEGER NOT NULL, address TEXT NOT NULL,
            block_number INTEGER NOT NULL, timestamp_utc TEXT NOT NULL,
            PRIMARY KEY (chain_id, transaction_hash, internal_index, address));
        CREATE INDEX IF NOT EXISTS source_creations_address
            ON source_creations (chain_id, address);
        CREATE TABLE IF NOT EXISTS source_window_creations (
            chain_id INTEGER NOT NULL, start_utc TEXT NOT NULL,
            transaction_hash TEXT NOT NULL, internal_index INTEGER NOT NULL,
            address TEXT NOT NULL,
            PRIMARY KEY (chain_id, start_utc, transaction_hash, internal_index, address),
            FOREIGN KEY (chain_id, start_utc)
                REFERENCES source_windows (chain_id, start_utc),
            FOREIGN KEY (chain_id, transaction_hash, internal_index, address)
                REFERENCES source_creations (chain_id, transaction_hash, internal_index, address));
    """)
    return db


def check_contiguous(db, chain_id, bootstrap, next_start, window_seconds):
    rows = db.execute("""SELECT start_utc, end_utc FROM source_windows
        WHERE chain_id=? ORDER BY start_utc""", (chain_id,))
    expected = bootstrap
    delta = dt.timedelta(seconds=window_seconds)
    for start_text, end_text in rows:
        start, end = parse_time(start_text), parse_time(end_text)
        if start != expected or end != start + delta:
            raise SourceError("Persisted source windows contain a gap or overlap")
        expected = end
    if expected != next_start:
        raise SourceError("Source checkpoint disagrees with persisted windows")


def prune_windows(db, chain_id, retain_windows):
    """Keep a recent, explicitly scoped range in a rolling source database."""
    if chain_id not in CHAINS or retain_windows < 1:
        raise ValueError("Invalid rolling retention")
    stream = db.execute("SELECT bootstrap_utc FROM streams WHERE chain_id=?",
                        (chain_id,)).fetchone()
    if stream is None:
        raise SourceError("Cannot prune an uninitialized source stream")
    oldest = db.execute("""SELECT start_utc FROM source_windows WHERE chain_id=?
        ORDER BY start_utc DESC LIMIT 1 OFFSET ?""",
        (chain_id, retain_windows - 1)).fetchone()
    if oldest is None or oldest[0] <= stream[0]:
        return stream[0]
    boundary = oldest[0]
    with db:
        db.execute("DELETE FROM source_window_creations WHERE chain_id=? AND start_utc<?",
                   (chain_id, boundary))
        db.execute("DELETE FROM source_windows WHERE chain_id=? AND start_utc<?",
                   (chain_id, boundary))
        db.execute("""DELETE FROM source_creations
            WHERE chain_id=? AND NOT EXISTS (
                SELECT 1 FROM source_window_creations membership
                WHERE membership.chain_id=source_creations.chain_id
                  AND membership.transaction_hash=source_creations.transaction_hash
                  AND membership.internal_index=source_creations.internal_index
                  AND membership.address=source_creations.address)""", (chain_id,))
        db.execute("UPDATE streams SET bootstrap_utc=? WHERE chain_id=?",
                   (boundary, chain_id))
    return boundary


def run(db, client, *, chain_id, bootstrap, window_seconds=300, max_windows=12,
        max_pages=1000, recheck_windows=12, now=None):
    if chain_id not in CHAINS or window_seconds < 1 or max_windows < 0 or (
            max_pages < 1 or recheck_windows < 0):
        raise ValueError("Invalid chain or indexing limits")
    bootstrap = parse_time(stamp(bootstrap))
    now = now or dt.datetime.now(UTC)
    safe_cutoff = parse_time(stamp(now - dt.timedelta(minutes=30)))
    existing = db.execute("SELECT bootstrap_utc, next_start_utc, window_seconds FROM streams "
                          "WHERE chain_id=?", (chain_id,)).fetchone()
    if existing is None:
        if bootstrap > safe_cutoff:
            raise SourceError("Bootstrap is newer than the safe source cutoff")
        with db:
            db.execute("INSERT INTO streams VALUES (?, ?, ?, ?)",
                       (chain_id, stamp(bootstrap), stamp(bootstrap), window_seconds))
        next_start = bootstrap
    else:
        if existing[0] != stamp(bootstrap) or existing[2] != window_seconds:
            raise SourceError("Bootstrap or window size differs from persisted stream")
        next_start = parse_time(existing[1])
    check_contiguous(db, chain_id, bootstrap, next_start, window_seconds)
    recent = db.execute("""SELECT start_utc, end_utc, creation_count, digest
        FROM source_windows WHERE chain_id=? ORDER BY start_utc DESC LIMIT ?""",
        (chain_id, recheck_windows)).fetchall()
    recheck_pages = 0
    for start_text, end_text, count, digest in recent:
        stored_rows = db.execute("""SELECT creation.transaction_hash,
            creation.internal_index, creation.address, creation.block_number,
            creation.timestamp_utc
            FROM source_window_creations membership
            JOIN source_creations creation
              ON creation.chain_id=membership.chain_id
             AND creation.transaction_hash=membership.transaction_hash
             AND creation.internal_index=membership.internal_index
             AND creation.address=membership.address
            WHERE membership.chain_id=? AND membership.start_utc=?
            ORDER BY creation.transaction_hash, creation.internal_index,
                     creation.address, creation.block_number, creation.timestamp_utc""",
            (chain_id, start_text)).fetchall()
        if len(stored_rows) != count or rows_digest(stored_rows) != digest:
            raise SourceError(f"Stored creations disagree with interval {start_text}")
        rows, pages, observed_digest = read_window(
            client, chain_id, parse_time(start_text), parse_time(end_text), max_pages=max_pages)
        recheck_pages += pages
        if len(rows) != count or observed_digest != digest:
            raise SourceError(f"Indexed source changed in completed interval {start_text}")

    completed = 0
    records = 0
    fetched_pages = 0
    delta = dt.timedelta(seconds=window_seconds)
    while completed < max_windows and next_start + delta <= safe_cutoff:
        end = next_start + delta
        rows, pages, digest = read_window(
            client, chain_id, next_start, end, max_pages=max_pages)
        with db:
            for transaction, internal, address, block, timestamp in rows:
                prior = db.execute("""SELECT block_number, timestamp_utc FROM source_creations
                    WHERE chain_id=? AND transaction_hash=? AND internal_index=? AND address=?""",
                    (chain_id, transaction, internal, address)).fetchone()
                if prior is not None and prior != (block, timestamp):
                    raise SourceError("Creation evidence changed across adjacent intervals")
                db.execute("""INSERT OR IGNORE INTO source_creations
                    VALUES (?, ?, ?, ?, ?, ?)""",
                    (chain_id, transaction, internal, address, block, timestamp))
            db.execute("INSERT INTO source_windows VALUES (?, ?, ?, ?, ?, ?, ?)",
                       (chain_id, stamp(next_start), stamp(end), pages, len(rows), digest,
                        stamp(dt.datetime.now(UTC))))
            db.executemany("""INSERT INTO source_window_creations
                VALUES (?, ?, ?, ?, ?)""",
                [(chain_id, stamp(next_start), transaction, internal, address)
                 for transaction, internal, address, _, _ in rows])
            db.execute("UPDATE streams SET next_start_utc=? WHERE chain_id=?",
                       (stamp(end), chain_id))
        next_start = end
        completed += 1
        records += len(rows)
        fetched_pages += pages
    return {
        "chain_id": chain_id, "status": "source_windows_verified",
        "bootstrap_utc": stamp(bootstrap), "next_start_utc": stamp(next_start),
        "safe_cutoff_utc": stamp(safe_cutoff), "windows_committed": completed,
        "creation_rows_observed": records, "pages_fetched": fetched_pages,
        "rechecked_windows": len(recent), "recheck_pages_fetched": recheck_pages,
        "unscanned_safe_seconds": max(0, int((safe_cutoff - next_start).total_seconds())),
        "source_windows_contiguous_from_bootstrap": True,
        "source_history_before_bootstrap_verified": False,
        "chain_wide_coverage": False, "create2_verified": False,
        "alert_eligible": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--chain-id", type=int, choices=CHAINS, required=True)
    parser.add_argument("--bootstrap-utc", required=True)
    parser.add_argument("--window-seconds", type=int, default=300)
    parser.add_argument("--max-windows", type=int, default=12)
    parser.add_argument("--max-pages", type=int, default=1000)
    parser.add_argument("--recheck-windows", type=int, default=12)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    try:
        client = Client(os.getenv("BLOCKSCOUT_API_KEY"))
        args.database.parent.mkdir(parents=True, exist_ok=True)
        with closing(open_store(args.database)) as db:
            report = run(db, client, chain_id=args.chain_id,
                         bootstrap=parse_time(args.bootstrap_utc),
                         window_seconds=args.window_seconds,
                         max_windows=args.max_windows,
                         max_pages=args.max_pages,
                         recheck_windows=args.recheck_windows)
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report))
    except (SourceError, sqlite3.Error, ValueError) as error:
        print(json.dumps({"status": "incomplete", "error": str(error),
                          "chain_wide_coverage": False, "alert_eligible": False}))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
