"""Evidence-gated, offline candidate assessment. Never sends notifications."""
import hashlib
import json
import sqlite3
from urllib.parse import urlparse

from radar.indexer import address

WEIGHTS = {
    "product": 20, "user_growth": 20, "onchain_activity": 20,
    "liquidity": 15, "revenue": 15, "risk": 10,
}
SOURCE_KINDS = {"project", "product", "token", "developer"}


def https_url(value):
    parsed = urlparse(value or "")
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError(f"Evidence requires a public HTTPS URL: {value!r}")
    return value


def assess(candidate, db, coverage):
    """Return a score and gate reasons; unverified metrics earn no points."""
    addr = address(candidate["address"])
    sources = candidate.get("sources", [])
    if not isinstance(sources, list):
        raise ValueError("sources must be a list")
    kinds = set()
    for source in sources:
        if source.get("kind") not in SOURCE_KINDS:
            raise ValueError("Unknown source kind")
        https_url(source.get("url"))
        if source.get("contract_address", "").lower() == addr:
            kinds.add(source["kind"])
    deployment = db.execute("SELECT 1 FROM deployments WHERE address=? LIMIT 1", (addr,)).fetchone()
    points = {}
    evidence = {}
    for name, weight in WEIGHTS.items():
        metric = candidate.get("metrics", {}).get(name)
        if metric is None:
            points[name] = 0
            continue
        if not isinstance(metric, dict) or type(metric.get("value")) not in (int, float):
            raise ValueError(f"Invalid {name} metric")
        value = metric["value"]
        if not 0 <= value <= 1:
            raise ValueError(f"{name} must be normalized to 0..1")
        url = https_url(metric.get("evidence_url"))
        if not metric.get("observed_at_utc"):
            raise ValueError(f"{name} needs an observation time")
        points[name] = round(value * weight, 2)
        evidence[name] = {"url": url, "observed_at_utc": metric["observed_at_utc"]}
    score = round(sum(points.values()), 2)
    reasons = []
    if not deployment:
        reasons.append("contract_not_in_verified_deployment_index")
    if "project" not in kinds or "product" not in kinds or "developer" not in kinds:
        reasons.append("official_project_product_developer_mapping_incomplete")
    if candidate.get("token_claim") and "token" not in kinds:
        reasons.append("token_claim_without_source")
    if coverage.get("historical_gap_before_bootstrap") or coverage.get("unscanned_safe_blocks") != 0:
        reasons.append("chain_coverage_incomplete")
    if not coverage.get("alert_eligible"):
        reasons.append("alerts_disabled_pending_validation")
    return {"address": addr, "score": score, "points": points,
            "metric_evidence": evidence, "matched_source_kinds": sorted(kinds),
            "eligible": not reasons, "reasons": reasons}


def propose_alert(db, project_id, assessment, minimum_score=70, improvement=10):
    """Idempotently store a proposal. Live delivery is intentionally absent."""
    db.execute("""CREATE TABLE IF NOT EXISTS alert_proposals (
        project_id TEXT NOT NULL, fingerprint TEXT NOT NULL, score REAL NOT NULL,
        payload TEXT NOT NULL, PRIMARY KEY(project_id, fingerprint))""")
    if not assessment["eligible"] or assessment["score"] < minimum_score:
        return False
    prior = db.execute("SELECT score FROM alert_proposals WHERE project_id=? ORDER BY rowid DESC LIMIT 1",
                       (project_id,)).fetchone()
    if prior and assessment["score"] - prior[0] < improvement:
        return False
    payload = json.dumps(assessment, sort_keys=True, ensure_ascii=False)
    fingerprint = hashlib.sha256(payload.encode()).hexdigest()
    with db:
        cursor = db.execute("INSERT OR IGNORE INTO alert_proposals VALUES (?, ?, ?, ?)",
                            (project_id, fingerprint, assessment["score"], payload))
    return cursor.rowcount == 1
