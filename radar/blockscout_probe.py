"""Read-only Blockscout creation-index capability check for both radar chains."""

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys
import urllib.error
import urllib.parse
import urllib.request


API_ROOT = "https://api.blockscout.com"
CHAINS = {"robinhood": 4663, "arc": 5042}
KNOWN_ROBINHOOD_BLOCK = 111009151
KNOWN_WINDOW = {"age_from": "2026-11-10T16:08:20Z",
                "age_to": "2026-11-10T16:08:35Z"}


class SourceError(RuntimeError):
    pass


def fetch(chain_id, path, params, key):
    if not key:
        raise SourceError("BLOCKSCOUT_API_KEY is missing")
    query = urllib.parse.urlencode({name: "null" if value is None else value
                                    for name, value in params.items()})
    url = f"{API_ROOT}/{chain_id}/api/v2/{path}"
    if query:
        url += "?" + query
    request = urllib.request.Request(url, headers={
        "Authorization": "Bearer " + key,
        "Accept": "application/json",
        "User-Agent": "chain-radar-blockscout-probe/1.0",
    })
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as error:
        raise SourceError(f"Blockscout chain {chain_id} {path}: HTTP {error.code}") from None
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as error:
        raise SourceError(f"Blockscout chain {chain_id} {path}: transport or JSON error "
                          f"({type(error).__name__})") from None
    if not isinstance(payload, dict):
        raise SourceError(f"Blockscout chain {chain_id} {path}: malformed response")
    return payload


def _creation_page(fetcher, chain_id, params, key):
    payload = fetcher(chain_id, "advanced-filters", params, key)
    items = payload.get("items")
    if not isinstance(items, list) or "next_page_params" not in payload:
        raise SourceError(f"Blockscout chain {chain_id}: incomplete creation page")
    if any(not isinstance(item, dict) or item.get("type") != "contract_creation"
           for item in items):
        raise SourceError(f"Blockscout chain {chain_id}: creation filter not honored")
    cursor = payload["next_page_params"]
    if cursor is not None and not isinstance(cursor, dict):
        raise SourceError(f"Blockscout chain {chain_id}: malformed pagination cursor")
    return items, cursor


def _identity(item):
    created = item.get("created_contract")
    address = created.get("hash") if isinstance(created, dict) else None
    return (item.get("hash"), item.get("internal_transaction_index"), address)


def probe(key, *, now=None, fetcher=fetch):
    now = now or datetime.now(timezone.utc)
    report = {"status": "incomplete", "checked_at_utc": now.isoformat(),
              "api_base": API_ROOT, "chains": {}, "errors": [],
              "coverage": {"chain_wide_discovery": False, "create2_verified": False,
                           "historical_contiguity_verified": False, "alert_eligible": False}}
    try:
        if not key:
            raise SourceError("BLOCKSCOUT_API_KEY is missing")
        end = now - timedelta(minutes=5)  # Avoid asserting indexed coverage at the tip.
        start = end - timedelta(hours=1)
        base = {"transaction_types": "CONTRACT_CREATION",
                "age_from": start.isoformat().replace("+00:00", "Z"),
                "age_to": end.isoformat().replace("+00:00", "Z")}
        for name, chain_id in CHAINS.items():
            stats = fetcher(chain_id, "stats", {}, key)
            if not stats:
                raise SourceError(f"Blockscout chain {chain_id}: empty stats")
            first, cursor = _creation_page(fetcher, chain_id, base, key)
            second = []
            if cursor is not None:
                second, _ = _creation_page(fetcher, chain_id, {**base, **cursor}, key)
                if not second or set(map(_identity, first)) & set(map(_identity, second)):
                    raise SourceError(f"Blockscout chain {chain_id}: pagination overlap or empty page")
            report["chains"][name] = {
                "chain_id": chain_id, "query_from_utc": base["age_from"],
                "query_to_utc": base["age_to"], "first_page_items": len(first),
                "second_page_items": len(second), "more_pages_unchecked": cursor is not None,
                "first_page_internal_creations": sum(
                    item.get("internal_transaction_index") is not None for item in first),
                "first_page_created_addresses": sum(
                    _identity(item)[2] is not None for item in first),
            }

        known, cursor = _creation_page(fetcher, CHAINS["robinhood"],
                                       {"transaction_types": "CONTRACT_CREATION",
                                        **KNOWN_WINDOW}, key)
        if cursor is not None:
            raise SourceError("Known Robinhood interval has an unfinished creation page")
        factory = [item for item in known
                   if item.get("block_number") == KNOWN_ROBINHOOD_BLOCK
                   and item.get("status") == "success"
                   and item.get("internal_transaction_index") is not None
                   and _identity(item)[2] is not None]
        if not factory:
            raise SourceError("Known Robinhood factory creation missing from indexed interval")
        report["known_factory_sample"] = {"block": KNOWN_ROBINHOOD_BLOCK,
                                           "internal_creations": len(factory),
                                           "interval_items": len(known)}
        report["status"] = "source_capability_verified"
    except (SourceError, TypeError, ValueError) as error:
        report["errors"].append(str(error))
    return report


def main():
    output = Path("out/blockscout_probe.json")
    report = probe(os.getenv("BLOCKSCOUT_API_KEY"))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"status": report["status"], "chains": list(report["chains"]),
                      "errors": report["errors"]}, ensure_ascii=False))
    return 0 if report["status"] == "source_capability_verified" else 1


if __name__ == "__main__":
    sys.exit(main())
