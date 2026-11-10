"""Human-reviewed evidence ledger. Candidate submissions cannot write this ledger."""
import datetime as dt
import hashlib
from urllib.parse import urlparse

from radar.indexer import address

SOURCE_KINDS = {"project", "product", "token", "developer"}
METRICS = {"product", "user_growth", "onchain_activity", "liquidity", "revenue", "risk"}


def https_url(value):
    parsed = urlparse(value or "")
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("Evidence URL must use public HTTPS without embedded credentials")
    return value


def utc_timestamp(value):
    stamp = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None or stamp.utcoffset() != dt.timedelta(0):
        raise ValueError("Observation time must include UTC timezone")
    if stamp > dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5):
        raise ValueError("Observation time is in the future")
    return stamp.isoformat()


def init_evidence(db):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS source_reviews (
            project_id TEXT NOT NULL, address TEXT NOT NULL, kind TEXT NOT NULL,
            url TEXT NOT NULL, snapshot_sha256 TEXT NOT NULL,
            reviewed_by TEXT NOT NULL, reviewed_at_utc TEXT NOT NULL,
            PRIMARY KEY (project_id, address, kind, url));
        CREATE TABLE IF NOT EXISTS metric_reviews (
            project_id TEXT NOT NULL, metric TEXT NOT NULL, value REAL NOT NULL,
            evidence_url TEXT NOT NULL, observed_at_utc TEXT NOT NULL,
            methodology TEXT NOT NULL, reviewed_by TEXT NOT NULL,
            reviewed_at_utc TEXT NOT NULL,
            PRIMARY KEY (project_id, metric, observed_at_utc, evidence_url));
    """)


def record_source_review(db, *, project_id, contract_address, kind, url,
                         snapshot, reviewed_by, reviewed_at_utc):
    """Store review of a captured first-party page that names this exact address."""
    if not project_id or not reviewed_by:
        raise ValueError("Project ID and independent reviewer are required")
    if kind not in SOURCE_KINDS:
        raise ValueError("Unknown source kind")
    addr = address(contract_address)
    https_url(url)
    if not isinstance(snapshot, str) or addr not in snapshot.lower():
        raise ValueError("Source snapshot does not name the contract address")
    reviewed_at = utc_timestamp(reviewed_at_utc)
    digest = hashlib.sha256(snapshot.encode()).hexdigest()
    init_evidence(db)
    with db:
        db.execute("""INSERT INTO source_reviews VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(project_id, address, kind, url) DO UPDATE SET
            snapshot_sha256=excluded.snapshot_sha256,
            reviewed_by=excluded.reviewed_by,
            reviewed_at_utc=excluded.reviewed_at_utc""",
            (project_id, addr, kind, url, digest, reviewed_by, reviewed_at))
    return digest


def record_metric_review(db, *, project_id, metric, value, evidence_url,
                         observed_at_utc, methodology, reviewed_by, reviewed_at_utc):
    """Store a reviewed normalized metric; methodology must explain the value."""
    if not project_id or not reviewed_by or not methodology:
        raise ValueError("Project ID, reviewer, and methodology are required")
    if metric not in METRICS or type(value) not in (int, float) or not 0 <= value <= 1:
        raise ValueError("Unknown or invalid normalized metric")
    https_url(evidence_url)
    observed = utc_timestamp(observed_at_utc)
    reviewed = utc_timestamp(reviewed_at_utc)
    if reviewed < observed:
        raise ValueError("Review cannot predate observation")
    init_evidence(db)
    with db:
        db.execute("""INSERT INTO metric_reviews VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(project_id, metric, observed_at_utc, evidence_url) DO UPDATE SET
            value=excluded.value, methodology=excluded.methodology,
            reviewed_by=excluded.reviewed_by,
            reviewed_at_utc=excluded.reviewed_at_utc""",
            (project_id, metric, value, evidence_url, observed, methodology,
             reviewed_by, reviewed))
