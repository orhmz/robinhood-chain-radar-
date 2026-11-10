"""Evidence-gated, offline candidate assessment. Never sends notifications."""
import hashlib
import json

from radar.indexer import address
from radar.evidence import init_evidence

WEIGHTS = {
    "product": 20, "user_growth": 20, "onchain_activity": 20,
    "liquidity": 15, "revenue": 15, "risk": 10,
}
def assess(candidate, db, coverage):
    """Return a score and gate reasons; unverified metrics earn no points."""
    addr = address(candidate["address"])
    project_id = candidate["project_id"]
    if not isinstance(project_id, str) or not project_id.strip():
        raise ValueError("Project ID required")
    init_evidence(db)
    source_rows = db.execute("""SELECT kind, url, snapshot_sha256, reviewed_by, reviewed_at_utc
        FROM source_reviews WHERE project_id=? AND address=?""", (project_id, addr)).fetchall()
    kinds = {row[0] for row in source_rows}
    deployment = db.execute("SELECT 1 FROM deployments WHERE address=? LIMIT 1", (addr,)).fetchone()
    points = {}
    evidence = {}
    for name, weight in WEIGHTS.items():
        row = db.execute("""SELECT value, evidence_url, observed_at_utc, methodology,
            reviewed_by, reviewed_at_utc FROM metric_reviews
            WHERE project_id=? AND metric=?
            ORDER BY observed_at_utc DESC, reviewed_at_utc DESC LIMIT 1""",
            (project_id, name)).fetchone()
        if row is None:
            points[name] = 0
            continue
        points[name] = round(row[0] * weight, 2)
        evidence[name] = {"url": row[1], "observed_at_utc": row[2],
                          "methodology": row[3], "reviewed_by": row[4],
                          "reviewed_at_utc": row[5]}
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
    return {"project_id": project_id, "address": addr, "score": score, "points": points,
            "metric_evidence": evidence, "matched_source_kinds": sorted(kinds),
            "source_reviews": [dict(zip(("kind", "url", "snapshot_sha256", "reviewed_by",
                                         "reviewed_at_utc"), row)) for row in source_rows],
            "eligible": not reasons, "reasons": reasons}


def propose_alert(db, project_id, assessment, minimum_score=70, improvement=10):
    """Idempotently store a proposal. Live delivery is intentionally absent."""
    db.execute("""CREATE TABLE IF NOT EXISTS alert_proposals (
        project_id TEXT NOT NULL, fingerprint TEXT NOT NULL, score REAL NOT NULL,
        payload TEXT NOT NULL, PRIMARY KEY(project_id, fingerprint))""")
    if (project_id != assessment["project_id"] or not assessment["eligible"] or
            assessment["reasons"] or assessment["score"] < minimum_score):
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
