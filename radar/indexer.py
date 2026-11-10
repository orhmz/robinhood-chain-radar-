"""Opt-in, trace-complete Robinhood Chain deployment indexer.

Requires an RPC node that serves debug_traceBlockByNumber with callTracer.
No alerts or project identity claims are emitted by this module.
"""
import argparse
import datetime as dt
import json
import os
import sqlite3
import time
from pathlib import Path

from radar.scan import rpc

CHAIN_ID = 4663
CONFIRMATIONS = 6


class CoverageError(RuntimeError):
    """A block could not be proved fully indexed."""


def quantity(value):
    if not isinstance(value, str) or not value.startswith("0x"):
        raise CoverageError(f"Invalid RPC quantity: {value!r}")
    return int(value, 16)


def address(value):
    if not isinstance(value, str) or len(value) != 42 or not value.startswith("0x"):
        raise CoverageError(f"Invalid contract address: {value!r}")
    int(value[2:], 16)
    return value.lower()


def parse_creations(node, tx_hash, block_number, path=(), ancestor_failed=False):
    """Walk a callTracer tree; reverted branches cannot yield deployments."""
    if not isinstance(node, dict):
        raise CoverageError("Malformed call trace node")
    if not isinstance(node.get("calls", []), list):
        raise CoverageError("Malformed child calls")
    failed = ancestor_failed or bool(node.get("error"))
    if not failed and node.get("type", "").upper() in ("CREATE", "CREATE2"):
        yield (block_number, tx_hash.lower(), json.dumps(path),
               address(node.get("to")), node["type"].upper(),
               "direct" if not path else "factory")
    for index, child in enumerate(node.get("calls", [])):
        yield from parse_creations(child, tx_hash, block_number, path + (index,), failed)


def open_store(path):
    db = sqlite3.connect(path)
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS coverage (
            height INTEGER PRIMARY KEY, hash TEXT NOT NULL, parent_hash TEXT NOT NULL,
            tx_count INTEGER NOT NULL, deployment_count INTEGER NOT NULL,
            verified_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS deployments (
            block_number INTEGER NOT NULL, transaction_hash TEXT NOT NULL,
            trace_path TEXT NOT NULL, address TEXT NOT NULL, method TEXT NOT NULL,
            origin TEXT NOT NULL,
            PRIMARY KEY (transaction_hash, trace_path));
        CREATE INDEX IF NOT EXISTS deployments_address ON deployments(address);
        CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    """)
    return db


def rpc_result(method, params=None):
    result, _ = rpc(method, params)
    if result is None:
        raise CoverageError(f"Null RPC response from {method}")
    return result


def index(db, start_block=None, max_blocks=100, call=rpc_result):
    """Commit only verified blocks. Retry/restart resumes at the first missing height."""
    if max_blocks < 1:
        raise ValueError("max_blocks must be positive")
    if quantity(call("eth_chainId")) != CHAIN_ID:
        raise CoverageError("RPC chain ID mismatch")
    head = quantity(call("eth_blockNumber"))
    safe_head = head - CONFIRMATIONS
    last = db.execute("SELECT height, hash FROM coverage ORDER BY height DESC LIMIT 1").fetchone()
    first = db.execute("SELECT value FROM metadata WHERE key='start_block'").fetchone()
    if first is None:
        if start_block is None or start_block < 0:
            raise CoverageError("Explicit --start-block required for bootstrap")
        if start_block > safe_head:
            raise CoverageError("Bootstrap block exceeds safe head")
        with db:
            db.execute("INSERT INTO metadata VALUES ('start_block', ?)", (str(start_block),))
        first_block = start_block
    else:
        first_block = int(first[0])
        if start_block is not None and start_block != first_block:
            raise CoverageError("Bootstrap start differs from persisted coverage")
    count, minimum, maximum = db.execute(
        "SELECT count(*), min(height), max(height) FROM coverage").fetchone()
    if count and (minimum != first_block or count != maximum - first_block + 1):
        raise CoverageError("Persisted coverage contains a missing block")
    if count and db.execute("""SELECT 1 FROM coverage current
        JOIN coverage previous ON previous.height = current.height - 1
        WHERE current.height > ? AND current.parent_hash != previous.hash LIMIT 1""",
        (first_block,)).fetchone():
        raise CoverageError("Persisted coverage contains a broken parent link")
    if last:
        anchor = call("eth_getBlockByNumber", [hex(last[0]), False])
        if not isinstance(anchor, dict) or anchor.get("hash", "").lower() != last[1]:
            raise CoverageError("Checkpoint anchor changed; possible reorg")
    next_block = last[0] + 1 if last else first_block
    target = min(safe_head, next_block + max_blocks - 1)
    processed = 0
    for height in range(next_block, target + 1):
        block = call("eth_getBlockByNumber", [hex(height), True])
        if not isinstance(block, dict) or quantity(block.get("number")) != height:
            raise CoverageError(f"Missing/inconsistent block {height}")
        block_hash = block.get("hash", "").lower()
        parent_hash = block.get("parentHash", "").lower()
        if len(block_hash) != 66 or len(parent_hash) != 66:
            raise CoverageError(f"Invalid block hash at {height}")
        if last and parent_hash != last[1]:
            raise CoverageError(f"Non-contiguous parent hash at {height}")
        txs = block.get("transactions")
        if not isinstance(txs, list) or any(not isinstance(tx, dict) or not tx.get("hash") for tx in txs):
            raise CoverageError(f"Incomplete transactions at {height}")
        # callTracer is required: ordinary receipts cannot reveal factory CREATE/CREATE2.
        traces = call("debug_traceBlockByNumber", [hex(height), {"tracer": "callTracer"}])
        if not isinstance(traces, list) or len(traces) != len(txs):
            raise CoverageError(f"Trace count mismatch at {height}")
        creations = []
        for tx, trace in zip(txs, traces):
            tx_hash = tx["hash"].lower()
            if not isinstance(trace, dict) or trace.get("txHash", "").lower() != tx_hash or "error" in trace:
                raise CoverageError(f"Missing/mismatched trace for {tx_hash}")
            tree = trace.get("result")
            if not isinstance(tree, dict):
                raise CoverageError(f"Malformed trace for {tx_hash}")
            found = list(parse_creations(tree, tx_hash, height))
            receipt = call("eth_getTransactionReceipt", [tx["hash"]])
            if (not isinstance(receipt, dict) or
                receipt.get("transactionHash", "").lower() != tx_hash or
                quantity(receipt.get("blockNumber")) != height or
                receipt.get("blockHash", "").lower() != block_hash):
                raise CoverageError(f"Invalid receipt for {tx_hash}")
            status = quantity(receipt.get("status"))
            if status not in (0, 1) or (status == 0 and found):
                raise CoverageError(f"Trace/receipt status mismatch for {tx_hash}")
            if tx.get("to") is None:
                direct = [row for row in found if row[2] == "[]"]
                if status == 1 and (len(direct) != 1 or
                                direct[0][3] != address(receipt.get("contractAddress"))):
                    raise CoverageError(f"Direct creation not corroborated by trace: {tx_hash}")
                if status == 0 and direct:
                    raise CoverageError(f"Reverted creation present in trace: {tx_hash}")
            creations.extend(found)
        # A reorg between block and trace requests must not produce a false proof.
        confirmation = call("eth_getBlockByNumber", [hex(height), False])
        if not isinstance(confirmation, dict) or confirmation.get("hash", "").lower() != block_hash:
            raise CoverageError(f"Block changed during trace at {height}")
        with db:
            db.executemany("INSERT INTO deployments VALUES (?, ?, ?, ?, ?, ?)", creations)
            db.execute("INSERT INTO coverage VALUES (?, ?, ?, ?, ?, ?)",
                       (height, block_hash, parent_hash, len(txs), len(creations),
                        dt.datetime.now(dt.timezone.utc).isoformat()))
        last = (height, block_hash)
        processed += 1
    return {
        "chain_id": CHAIN_ID, "bootstrap_block": first_block,
        "last_verified_block": last[0] if last else None,
        "safe_head": safe_head, "blocks_processed": processed,
        "unscanned_safe_blocks": max(0, safe_head - (last[0] if last else first_block - 1)),
        "historical_gap_before_bootstrap": first_block > 0,
        "creation_coverage": "direct_and_traced_factory_create_create2",
        "alert_eligible": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--start-block", type=int)
    parser.add_argument("--max-blocks", type=int, default=100)
    parser.add_argument("--follow", action="store_true", help="continue processing new safe blocks")
    parser.add_argument("--poll-seconds", type=float, default=2)
    args = parser.parse_args()
    args.database.parent.mkdir(parents=True, exist_ok=True)
    db = open_store(args.database)
    try:
        if args.poll_seconds <= 0:
            parser.error("--poll-seconds must be positive")
        while True:
            report = index(db, args.start_block, args.max_blocks)
            print(json.dumps(report), flush=True)
            if not args.follow:
                break
            args.start_block = None
            if report["unscanned_safe_blocks"] == 0:
                time.sleep(args.poll_seconds)
    finally:
        db.close()


if __name__ == "__main__":
    main()
