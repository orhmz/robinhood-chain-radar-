"""Conservative Robinhood Chain (4663) data health check; stdlib Python 3.12."""
import datetime as dt
import json
import os
from pathlib import Path
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

UTC = dt.timezone.utc
BASE = os.getenv("BLOCKSCOUT_BASE", "https://robinhoodchain.blockscout.com").rstrip("/")
OUT = Path("out")
OUT.mkdir(exist_ok=True)
STATE = Path("state.json")

def utc_now():
    return dt.datetime.now(UTC)

def fetch(path, params=None):
    url = BASE + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Robinhood-Chain-Data-Health/1.0",
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=20) as response:
                return json.load(response), url
        except (OSError, TimeoutError, ValueError) as exc:
            if attempt == 2:
                raise RuntimeError(f"{url}: {type(exc).__name__}: {exc}") from exc
            time.sleep(2 ** attempt)

def parse_time(value):
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)

def rpc(method, params=None):
    url = os.getenv("ROBINHOOD_RPC_URL", "https://rpc.mainnet.chain.robinhood.com")
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}).encode()
    req = urllib.request.Request(url, data=body, headers={
        "Content-Type": "application/json", "Accept": "application/json",
        "User-Agent": "Robinhood-Chain-Data-Health/1.1",
    })
    with urllib.request.urlopen(req, timeout=25) as response:
        data = json.load(response)
    if "error" in data or "result" not in data:
        raise RuntimeError("RPC returned an error or missing result")
    return data["result"], url

def main():
    started = utc_now()
    report = {
        "checked_at_utc": started.isoformat(),
        "chain_id": 4663,
        "status": "incomplete",
        "coverage": "NO contiguous contract creation or transaction/log range verified",
        "sources": [],
        "candidates": [],
        "errors": [],
    }
    try:
        previous = json.loads(STATE.read_text()) if STATE.exists() else {}
        chain_id, rpc_url = rpc("eth_chainId")
        if int(chain_id, 16) != 4663:
            raise ValueError("RPC chain ID mismatch")
        head_hex, _ = rpc("eth_blockNumber")
        number = int(head_hex, 16)
        block, _ = rpc("eth_getBlockByNumber", [head_hex, False])
        if not isinstance(block, dict) or int(block["number"], 16) != number:
            raise ValueError("RPC block response inconsistent")
        stamp = dt.datetime.fromtimestamp(int(block["timestamp"], 16), UTC)
        if stamp > utc_now() + dt.timedelta(minutes=5) or utc_now() - stamp > dt.timedelta(hours=2):
            raise ValueError("RPC block timestamp future or stale")
        report["sources"].append({
            "url": rpc_url, "accessed_at_utc": utc_now().isoformat(),
            "data_end_utc": stamp.isoformat(),
            "scope": "RPC chain ID, latest block number and timestamp only",
        })
        prior = previous.get("last_observed_head")
        if prior is not None and number < int(prior):
            raise ValueError("Indexed head regressed relative to saved observation")
        report["head"] = {"number": number, "timestamp_utc": stamp.isoformat()}
        try:
            feed, feed_url = fetch("/api/v2/smart-contracts", {"filter": "verified"})
            entries = feed.get("items") if isinstance(feed, dict) else None
            if not isinstance(entries, list):
                raise ValueError("Verified contract response invalid")
            report["sources"].append({
                "url": feed_url, "accessed_at_utc": utc_now().isoformat(),
                "data_end_utc": "unknown",
                "scope": "first page only; NOT complete deployment coverage",
                "has_more_pages": bool(feed.get("next_page_params")),
            })
            for entry in entries:
                addr = entry.get("address") or {}
                address = addr.get("hash") if isinstance(addr, dict) else addr
                if address:
                    report["candidates"].append({
                        "address": address, "name": entry.get("name"),
                        "status": "UNVERIFIED project identity; not an alert",
                    })
        except Exception as exc:
            report["errors"].append("Optional Blockscout discovery unavailable: " + str(exc))
        report["status"] = "rpc_healthy_discovery_incomplete"
        # A head observation is NOT a checkpoint of complete contract coverage.
        STATE.write_text(json.dumps({
            "last_observed_head": number,
            "observed_at_utc": utc_now().isoformat(),
            "head_timestamp_utc": stamp.isoformat(),
            "last_fully_scanned_contract_block": None,
        }, indent=2) + "\n")
    except Exception as exc:
        report["errors"].append(str(exc))
    finally:
        (OUT / "status.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        print(json.dumps({
            "status": report["status"],
            "head": report.get("head"),
            "sample_candidates": len(report["candidates"]),
            "errors": report["errors"],
        }, ensure_ascii=False))
    if report["status"] == "incomplete":
        sys.exit(1)

if __name__ == "__main__":
    main()
