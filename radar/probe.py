"""Read-only RPC capability preflight; never advances a coverage checkpoint."""
import argparse
import json
import os
import sys
import urllib.error
import urllib.request

DEFAULT_RPC = "https://rpc.mainnet.chain.robinhood.com"


def call(method, params=None, opener=urllib.request.urlopen):
    url = os.getenv("ROBINHOOD_RPC_URL", DEFAULT_RPC)
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                       "params": params or []}).encode()
    request = urllib.request.Request(url, data=body, headers={
        "Content-Type": "application/json", "Accept": "application/json",
        "User-Agent": "Robinhood-Chain-Data-Health/1.1",
    })
    with opener(request, timeout=25) as response:
        payload = json.load(response)
    if not isinstance(payload, dict) or payload.get("error"):
        error = payload.get("error", {}) if isinstance(payload, dict) else {}
        raise RuntimeError(f"{method}: RPC error {error.get('code')}: {error.get('message')}")
    if payload.get("result") is None:
        raise RuntimeError(f"{method}: missing RPC result")
    return payload["result"]


def probe(block=None, rpc=call):
    report = {"chain_id": 4663, "trace_method": "debug_traceBlockByNumber",
              "trace_supported": False, "source": "configured" if os.getenv("ROBINHOOD_RPC_URL") else "official_public"}
    try:
        chain = int(rpc("eth_chainId"), 16)
        if chain != 4663:
            raise RuntimeError(f"unexpected chain ID {chain}")
        head = int(rpc("eth_blockNumber"), 16)
        height = head - 6 if block is None else block
        if height < 0 or height > head - 6:
            raise RuntimeError("probe block must be at or below the six-block safe head")
        block_data = rpc("eth_getBlockByNumber", [hex(height), False])
        if not isinstance(block_data, dict) or int(block_data.get("number", "0x0"), 16) != height:
            raise RuntimeError("block unavailable or inconsistent")
        hashes = block_data.get("transactions")
        if not isinstance(hashes, list):
            raise RuntimeError("transaction list unavailable")
        traces = rpc("debug_traceBlockByNumber", [hex(height), {"tracer": "callTracer"}])
        if not isinstance(traces, list) or len(traces) != len(hashes):
            raise RuntimeError("trace count does not match transaction count")
        for expected, item in zip(hashes, traces):
            if (not isinstance(item, dict) or item.get("txHash", "").lower() != expected.lower()
                    or not isinstance(item.get("result"), dict) or item.get("error")):
                raise RuntimeError("trace does not match transaction list")
        report.update({"trace_supported": True, "checked_block": height,
                       "transaction_count": len(hashes), "head": head})
    except (RuntimeError, ValueError, TypeError, urllib.error.URLError, TimeoutError) as exc:
        report["error"] = str(exc)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--block", type=int)
    args = parser.parse_args()
    report = probe(args.block)
    print(json.dumps(report))
    if not report["trace_supported"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
