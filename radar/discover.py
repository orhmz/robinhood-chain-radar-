"""Small, auditable RPC contract-creation sample. Not exhaustive chain indexing."""
import datetime as dt
import json
import os
from pathlib import Path
import sys
import time
from radar.scan import rpc, utc_now

OUT = Path("out")
OUT.mkdir(exist_ok=True)
SAMPLE_BLOCKS = min(max(int(os.getenv("SAMPLE_BLOCKS", "12")), 1), 20)
CHECKPOINT = Path("discovery_checkpoint.json")

def main():
    result = {
        "status": "incomplete",
        "checked_at_utc": utc_now().isoformat(),
        "chain_id": 4663,
        "scope": "Only top-level contract creation transactions (to=null) in a bounded recent block sample. Excludes internal CREATE/CREATE2, factories and older blocks.",
        "blocks": [],
        "deployments": [],
        "errors": [],
    }
    try:
        chain, _ = rpc("eth_chainId")
        if int(chain, 16) != 4663:
            raise RuntimeError("Unexpected RPC chain ID")
        head_hex, _ = rpc("eth_blockNumber")
        head = int(head_hex, 16)
        # Wait for six confirmations, so latest/reorg-sensitive blocks are excluded.
        safe_head = head - 6
        checkpoint = json.loads(CHECKPOINT.read_text()) if CHECKPOINT.exists() else {}
        prior_end = checkpoint.get("last_scanned_block")
        if prior_end is None:
            end = safe_head
            start = end - SAMPLE_BLOCKS + 1
        else:
            anchor, _ = rpc("eth_getBlockByNumber", [hex(int(prior_end)), False])
            if not isinstance(anchor, dict) or anchor.get("hash", "").lower() != checkpoint.get("last_scanned_block_hash", "").lower():
                raise RuntimeError("Checkpoint anchor block hash mismatch: possible reorg; refusing to advance")
            start = int(prior_end) + 1
            end = min(start + SAMPLE_BLOCKS - 1, safe_head)
            if start > safe_head:
                result["status"] = "caught_up_no_new_finalized_blocks"
                result["coverage"] = {"last_scanned_block": int(prior_end), "safe_head": safe_head, "chain_wide_coverage": False}
                return
        if start < 0:
            raise RuntimeError("Chain too short")
        for height in range(start, end + 1):
            block, _ = rpc("eth_getBlockByNumber", [hex(height), True])
            if not isinstance(block, dict) or int(block["number"], 16) != height:
                raise RuntimeError(f"Missing or inconsistent block {height}")
            txs = block.get("transactions")
            if not isinstance(txs, list):
                raise RuntimeError(f"Transactions unavailable at block {height}")
            created = []
            for tx in txs:
                if tx.get("to") is not None:
                    continue
                receipt, _ = rpc("eth_getTransactionReceipt", [tx["hash"]])
                if not isinstance(receipt, dict) or receipt.get("transactionHash", "").lower() != tx["hash"].lower():
                    raise RuntimeError(f"Missing receipt for {tx['hash']}")
                if int(receipt.get("blockNumber", "0x0"), 16) != height:
                    raise RuntimeError(f"Receipt block mismatch {tx['hash']}")
                address = receipt.get("contractAddress")
                if int(receipt.get("status", "0x0"), 16) == 1 and address:
                    created.append({
                        "address": address,
                        "transaction_hash": tx["hash"],
                        "deployer": tx.get("from"),
                        "block": height,
                        "classification": "top-level contract deployment only; project identity and token status NOT verified",
                    })
                time.sleep(0.1)
            result["blocks"].append({
                "height": height, "hash": block["hash"],
                "transaction_count": len(txs),
                "top_level_deployments": len(created),
            })
            result["deployments"].extend(created)
            time.sleep(0.15)
        result["coverage"] = {
            "from_block": start, "to_block": end,
            "blocks_verified": len(result["blocks"]),
            "contiguous": True,
            "limited_to": "top-level contract creation transactions only",
            "historical_gap_before_bootstrap": True,
            "unscanned_finalized_blocks_after_end": safe_head - end,
            "gap_after_end_to_head": head - end,
            "chain_wide_coverage": False,
        }
        result["status"] = "incremental_partial_chain_coverage"
        result["coverage"]["backlog_status"] = (
            "caught_up" if safe_head == end else "behind_chain_tip"
        )
        result["coverage"]["alert_eligible"] = False
        result["coverage"]["reason"] = (
            "No full contract coverage: factory/internal deployments excluded, "
            "historical bootstrap gap exists, and block backlog may remain"
        )
        CHECKPOINT.write_text(json.dumps({
            "chain_id": 4663,
            "last_scanned_block": end,
            "last_scanned_block_hash": result["blocks"][-1]["hash"],
            "updated_at_utc": utc_now().isoformat(),
            "historical_gap_before_bootstrap": True,
            "coverage_type": "top_level_contract_creation_only",
        }, indent=2) + "\n")
    except Exception as exc:
        result["errors"].append(str(exc))
    finally:
        (OUT / "deployments_sample.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
        print(json.dumps({
            "status": result["status"],
            "coverage": result.get("coverage"),
            "deployments": len(result["deployments"]),
            "errors": result["errors"],
        }, ensure_ascii=False))
    if result["status"] not in ("incremental_partial_chain_coverage", "caught_up_no_new_finalized_blocks"):
        sys.exit(1)

if __name__ == "__main__":
    main()
