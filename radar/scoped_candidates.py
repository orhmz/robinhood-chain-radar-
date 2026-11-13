"""Bounded, read-only verification of Blockscout creation suggestions.

This is a contract evidence queue, not project attribution or an alert feed.
Internal creation links remain Blockscout-sourced without trace verification.
"""

from contextlib import closing
import datetime as dt
import json
import os
from pathlib import Path
import re
import sqlite3
import sys

from radar.arc_probe import SourceError as ArcRpcError, rpc as arc_rpc
from radar.blockscout_probe import SourceError, fetch
from radar.blockscout_indexer import parse_time
from radar.rpc_client import RpcError, call as robinhood_rpc


CHAINS = (4663, 5042)
DATABASE = Path("out/rolling-blockscout.sqlite")
REPORT = Path("out/scoped-candidates.json")


def _hex_int(value):
    if not isinstance(value, str) or not value.startswith("0x"):
        return None
    try:
        return int(value, 16)
    except ValueError:
        return None


def verify_row(row, *, key, fetcher, rpc):
    chain_id, tx_hash, internal_index, address, block_number = row
    address_detail = fetcher(chain_id, f"addresses/{address}", {}, key)
    if not isinstance(address_detail, dict) or address_detail.get("is_verified") is not True:
        return "unverified_source", None
    contract_detail = fetcher(chain_id, f"smart-contracts/{address}", {}, key)
    if not isinstance(contract_detail, dict) or contract_detail.get("is_fully_verified") is not True:
        return "not_fully_verified", None
    if (address_detail.get("creation_transaction_hash") or "").lower() != tx_hash:
        return "creation_link_mismatch", None
    receipt = rpc("eth_getTransactionReceipt", [tx_hash])
    if not isinstance(receipt, dict) or receipt.get("status") != "0x1" or (
            _hex_int(receipt.get("blockNumber")) != block_number):
        return "receipt_mismatch", None
    code = rpc("eth_getCode", [address, "latest"])
    if not isinstance(code, str) or not re.fullmatch(r"0x(?:[0-9a-fA-F]{2})+", code):
        return "no_current_code", None
    if internal_index == -1:
        if (receipt.get("contractAddress") or "").lower() != address:
            return "direct_receipt_address_mismatch", None
        level = "direct_receipt_and_current_code"
    else:
        if receipt.get("contractAddress") is not None:
            return "internal_receipt_inconsistent", None
        level = "internal_link_from_blockscout_pending_trace"
    return "evidence_recorded", {
        "chain_id": chain_id, "address": address, "creation_transaction_hash": tx_hash,
        "creation_block": block_number, "creation_kind": (
            "direct" if internal_index == -1 else "internal_create_method_unknown"),
        "verification_level": level,
        "source_code_fully_verified": True, "current_code_present": True,
        "receipt_success_and_block_match": True,
        "project_identity_verified": False, "token_identity_verified": False,
        "opportunity_score": None, "alert_eligible": False,
    }


def collect(db, key, *, fetcher=fetch, rpc_by_chain=None, limit=12,
            now=None):
    if not key or limit < 1 or limit > 50:
        raise SourceError("Missing API key or invalid candidate limit")
    rpc_by_chain = rpc_by_chain or {4663: robinhood_rpc, 5042: arc_rpc}
    now = now or dt.datetime.now(dt.timezone.utc)
    report = {
        "status": "incomplete", "checked_at_utc": now.isoformat(),
        "selection_limit_per_chain": limit, "source_scope": "recent_rolling_blockscout_windows",
        "chains": {}, "contract_evidence": [],
        "chain_wide_coverage": False, "create2_verified": False,
        "project_identity_verified": False, "alert_eligible": False,
    }
    for chain_id in CHAINS:
        rpc = rpc_by_chain[chain_id]
        if _hex_int(rpc("eth_chainId")) != chain_id:
            raise SourceError(f"Unexpected RPC chain ID for {chain_id}")
        rows = db.execute("""SELECT chain_id, transaction_hash, internal_index,
                address, block_number FROM source_creations WHERE chain_id=?
                ORDER BY block_number DESC, transaction_hash DESC LIMIT ?""",
                (chain_id, limit)).fetchall()
        counts = {"selected": len(rows), "evidence_recorded": 0,
                  "unverified_source": 0, "not_fully_verified": 0,
                  "creation_link_mismatch": 0, "receipt_mismatch": 0,
                  "no_current_code": 0, "direct_receipt_address_mismatch": 0,
                  "internal_receipt_inconsistent": 0}
        for row in rows:
            reason, evidence = verify_row(row, key=key, fetcher=fetcher, rpc=rpc)
            counts[reason] += 1
            if evidence:
                report["contract_evidence"].append(evidence)
        report["chains"][str(chain_id)] = counts
    report["status"] = "bounded_contract_evidence_collected"
    return report


def source_freshness(db, now, max_age=dt.timedelta(hours=4)):
    rows = db.execute("SELECT chain_id, next_start_utc FROM streams").fetchall()
    if {row[0] for row in rows} != set(CHAINS):
        raise SourceError("Source stream checkpoint missing")
    cutoffs = {str(chain_id): next_start for chain_id, next_start in rows}
    fresh = all(dt.timedelta() <= now - parse_time(value) <= max_age
                for value in cutoffs.values())
    return fresh, cutoffs


def main():
    try:
        with closing(sqlite3.connect(f"file:{DATABASE}?mode=ro", uri=True)) as db:
            now = dt.datetime.now(dt.timezone.utc)
            fresh, cutoffs = source_freshness(db, now)
            report = collect(db, os.getenv("BLOCKSCOUT_API_KEY"), now=now)
            report["source_next_start_utc"] = cutoffs
            report["source_fresh_within_four_hours"] = fresh
            if not fresh:
                report["status"] = "stale_source_evidence_only"
    except (SourceError, ArcRpcError, RpcError, sqlite3.Error, OSError,
            ValueError, TypeError, KeyError) as error:
        report = {"status": "incomplete", "error_type": type(error).__name__,
                  "chain_wide_coverage": False, "alert_eligible": False}
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"status": report["status"], "chain_counts": report.get("chains", {}),
                      "error_type": report.get("error_type")}))
    return 0 if report["status"] == "bounded_contract_evidence_collected" else 1


if __name__ == "__main__":
    sys.exit(main())
