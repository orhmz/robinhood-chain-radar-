"""Read-only Arc RPC evidence for known contracts; no chain-wide discovery."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import urllib.error
import urllib.request


RPC_URL = "https://rpc.mainnet.arc.io"
CHAIN_ID = 5042
CONFIRMATIONS = 64
BOUNTY_ADAPTER = "0x73c617e808ED5c7Ca41413DFC6EE940dDcBb0b8D"
KNOWN_CONTRACTS = {
    "bounty_adapter": BOUNTY_ADAPTER,
    "escrow_proxy": "0x64cA39Fc57315D0D488acCaC07c37C6E841CD058",
    "escrow_implementation": "0x3BD209158283FE1cd81054b1c91Ea723f1eBB251",
}
KNOWN_CREATION_BLOCK = 21153190
KNOWN_CREATION_TX = "0x89b0987ffa8a0b2632896dab69251d3f4d70343b65e99dc517e526d7376a1d45"


class SourceError(RuntimeError):
    pass


def rpc(method, params=None):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                       "params": params or []}).encode()
    request = urllib.request.Request(RPC_URL, data=body, headers={
        "Content-Type": "application/json", "Accept": "application/json",
        "User-Agent": "arc-radar-probe/1.0",
    })
    try:
        with urllib.request.urlopen(request, timeout=25) as response:
            payload = json.load(response)
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as error:
        raise SourceError(f"{method}: transport or JSON error ({type(error).__name__})") from None
    if not isinstance(payload, dict) or payload.get("error"):
        raise SourceError(f"{method}: RPC error or invalid response")
    return payload.get("result")


def collect(call=rpc):
    report = {
        "status": "incomplete", "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": RPC_URL, "chain_id": CHAIN_ID,
        "coverage": {"known_contracts_only": True, "chain_wide_discovery": False,
                     "factory_create2_discovery": False, "alert_eligible": False},
        "known_contracts": {}, "errors": [],
    }
    try:
        if int(call("eth_chainId"), 16) != CHAIN_ID:
            raise SourceError("Unexpected chain ID")
        head = int(call("eth_blockNumber"), 16)
        safe = head - CONFIRMATIONS
        if safe < KNOWN_CREATION_BLOCK:
            raise SourceError("Safe head is before the known creation block")
        block = call("eth_getBlockByNumber", [hex(safe), False])
        if not isinstance(block, dict) or int(block.get("number", "0x0"), 16) != safe:
            raise SourceError("Safe block unavailable or inconsistent")
        report["head"] = head
        report["last_checked_block"] = safe
        report["last_checked_block_hash"] = block.get("hash")

        for label, address in KNOWN_CONTRACTS.items():
            code = call("eth_getCode", [address, hex(safe)])
            if not isinstance(code, str) or not code.startswith("0x") or len(code) <= 2:
                raise SourceError(f"Known {label} has no bytecode at the checked block")
            report["known_contracts"][label] = {"address": address,
                                                   "code_bytes": (len(code) - 2) // 2,
                                                   "code_checked_at_block": safe}

        creation_block = call("eth_getBlockByNumber", [hex(KNOWN_CREATION_BLOCK), False])
        if (not isinstance(creation_block, dict)
                or int(creation_block.get("number", "0x0"), 16) != KNOWN_CREATION_BLOCK
                or not isinstance(creation_block.get("transactions"), list)):
            raise SourceError("Known historical block unavailable or inconsistent")
        report["known_creation_tx_in_block"] = (KNOWN_CREATION_TX.lower() in
                                                 {tx.lower() for tx in creation_block["transactions"]})
        if not report["known_creation_tx_in_block"]:
            raise SourceError("Known creation transaction absent from historical block")
        receipt = call("eth_getTransactionReceipt", [KNOWN_CREATION_TX])
        report["known_creation_receipt_verified"] = bool(
            isinstance(receipt, dict) and receipt.get("status") == "0x1"
            and (receipt.get("contractAddress") or "").lower() == BOUNTY_ADAPTER.lower()
            and int(receipt.get("blockNumber", "0x0"), 16) == KNOWN_CREATION_BLOCK)
        report["known_creation_receipt_note"] = (
            "Verified on this RPC" if report["known_creation_receipt_verified"]
            else "Unavailable or inconsistent on this RPC; creation not verified by this run")

        # This proves only a bounded query for one known address, not all Arc deployments.
        start = safe - 100
        logs = call("eth_getLogs", [{"fromBlock": hex(start), "toBlock": hex(safe),
                                     "address": BOUNTY_ADAPTER}])
        if not isinstance(logs, list):
            raise SourceError("Known-address log query incomplete")
        report["known_address_log_window"] = {"from_block": start, "to_block": safe,
                                                "log_count": len(logs)}
        report["status"] = "known_contract_code_verified"
    except (SourceError, TypeError, ValueError, KeyError) as error:
        report["errors"].append(str(error))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("out/arc_probe.json"))
    args = parser.parse_args()
    report = collect()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"status": report["status"], "last_checked_block": report.get("last_checked_block"),
                      "errors": report["errors"]}, ensure_ascii=False))
    return 0 if report["status"] == "known_contract_code_verified" else 1


if __name__ == "__main__":
    sys.exit(main())
