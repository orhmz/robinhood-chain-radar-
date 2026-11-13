"""Read-only Blockscout verified-contract metadata capability check.

Reports counts and field names only. It never labels an address as a project
or token and never advances a checkpoint or sends notifications.
"""

from contextlib import closing
import datetime as dt
import json
import os
from pathlib import Path
import re
import sqlite3
import sys

from radar.blockscout_probe import SourceError, fetch


CHAINS = (4663, 5042)
DATABASE = Path("out/rolling-blockscout.sqlite")
REPORT = Path("out/verified-source-probe.json")


def _field_names(value):
    return sorted(value) if isinstance(value, dict) else []


def _error_kind(error):
    match = re.search(r"HTTP (\d{3})", str(error))
    return f"HTTP {match.group(1)}" if match else "transport_or_malformed_response"


def probe(db, key, fetcher=fetch):
    report = {
        "status": "incomplete",
        "checked_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "chains": {}, "errors": [],
        "project_identity_verified": False,
        "token_identity_verified": False,
        "chain_wide_coverage": False,
        "alert_eligible": False,
    }
    if not key:
        report["errors"].append("BLOCKSCOUT_API_KEY is missing")
        return report
    for chain_id in CHAINS:
        result = {
            "chain_id": chain_id,
            "verified_list_accessible": False,
            "verified_list_first_page_items": None,
            "verified_list_more_pages": None,
            "verified_list_item_fields": [],
            "sampled_source_addresses": 0,
            "sampled_smart_contract_details": 0,
            "sampled_address_details": 0,
            "sampled_verified_flags": 0,
            "smart_contract_detail_fields": [],
            "address_detail_fields": [],
            "errors": [],
        }
        try:
            # The unified API returns HTTP 422 for filter=verified. Its
            # unfiltered endpoint lists verified contracts by definition;
            # still inspect each address before treating it as evidence.
            page = fetcher(chain_id, "smart-contracts", {}, key)
            items = page.get("items") if isinstance(page, dict) else None
            if not isinstance(items, list) or "next_page_params" not in page:
                raise SourceError("Verified smart-contract list is malformed")
            result["verified_list_accessible"] = True
            result["verified_list_first_page_items"] = len(items)
            result["verified_list_more_pages"] = page["next_page_params"] is not None
            result["verified_list_item_fields"] = _field_names(items[0]) if items else []
        except SourceError as error:
            result["errors"].append(f"verified list: {_error_kind(error)}")

        rows = db.execute("""SELECT address FROM source_creations
            WHERE chain_id=? GROUP BY address ORDER BY max(block_number) DESC LIMIT 3""",
            (chain_id,)).fetchall()
        result["sampled_source_addresses"] = len(rows)
        for (address,) in rows:
            try:
                detail = fetcher(chain_id, f"smart-contracts/{address}", {}, key)
                result["sampled_smart_contract_details"] += 1
                result["smart_contract_detail_fields"] = _field_names(detail)
            except SourceError as error:
                if "HTTP 404" not in str(error):
                    result["errors"].append(f"smart contract detail: {_error_kind(error)}")
            try:
                detail = fetcher(chain_id, f"addresses/{address}", {}, key)
                result["sampled_address_details"] += 1
                result["address_detail_fields"] = _field_names(detail)
                if isinstance(detail, dict) and detail.get("is_verified") is True:
                    result["sampled_verified_flags"] += 1
            except SourceError as error:
                result["errors"].append(f"address detail: {_error_kind(error)}")
        report["chains"][str(chain_id)] = result
    if all(item["verified_list_accessible"] or item["sampled_address_details"]
           for item in report["chains"].values()):
        report["status"] = "metadata_api_capability_observed"
    else:
        report["errors"].append("Verified-contract metadata unavailable on at least one chain")
    return report


def main():
    try:
        with closing(sqlite3.connect(f"file:{DATABASE}?mode=ro", uri=True)) as db:
            report = probe(db, os.getenv("BLOCKSCOUT_API_KEY"))
    except (sqlite3.Error, OSError) as error:
        report = {"status": "incomplete", "errors": [type(error).__name__],
                  "project_identity_verified": False, "alert_eligible": False}
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"status": report["status"], "errors": report.get("errors", []),
                      "chain_ids": list(report.get("chains", {}))}))
    return 0 if report["status"] == "metadata_api_capability_observed" else 1


if __name__ == "__main__":
    sys.exit(main())
