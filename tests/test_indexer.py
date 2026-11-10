import tempfile
import unittest
from pathlib import Path

from radar.indexer import CoverageError, index, open_store
from radar.assessment import assess, propose_alert
from radar.evidence import record_metric_review, record_source_review


BLOCK_HASH = "0x" + "a" * 64
PARENT_HASH = "0x" + "b" * 64
TX1 = "0x" + "1" * 64
TX2 = "0x" + "2" * 64
DIRECT = "0x" + "3" * 40
FACTORY = "0x" + "4" * 40


def fixture_call(method, params=None):
    if method == "eth_chainId":
        return hex(4663)
    if method == "eth_blockNumber":
        return hex(16)  # safe head = 10
    if method == "eth_getBlockByNumber":
        if params[1] is False:
            return {"hash": BLOCK_HASH}
        return {"number": hex(10), "hash": BLOCK_HASH, "parentHash": PARENT_HASH,
                "transactions": [{"hash": TX1, "to": None}, {"hash": TX2, "to": FACTORY}]}
    if method == "debug_traceBlockByNumber":
        return [
            {"txHash": TX1, "result": {"type": "CREATE", "to": DIRECT}},
            {"txHash": TX2, "result": {"type": "CALL", "calls": [
                {"type": "CREATE2", "to": FACTORY},
                {"type": "CREATE", "to": DIRECT, "error": "reverted"}]}}
        ]
    if method == "eth_getTransactionReceipt":
        return {"transactionHash": TX1, "blockNumber": hex(10),
                "blockHash": BLOCK_HASH, "status": "0x1", "contractAddress": DIRECT}
    raise AssertionError(method)


class IndexerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = open_store(Path(self.tmp.name) / "index.sqlite")

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def test_direct_factory_create2_and_resume(self):
        report = index(self.db, start_block=10, call=fixture_call)
        self.assertEqual(report["blocks_processed"], 1)
        self.assertEqual(report["unscanned_safe_blocks"], 0)
        self.assertTrue(report["historical_gap_before_bootstrap"])
        rows = self.db.execute("SELECT address, method, origin FROM deployments ORDER BY transaction_hash").fetchall()
        self.assertEqual(rows, [(DIRECT, "CREATE", "direct"), (FACTORY, "CREATE2", "factory")])
        self.assertEqual(index(self.db, call=fixture_call)["blocks_processed"], 0)

    def test_partial_trace_never_commits_coverage(self):
        def incomplete(method, params=None):
            result = fixture_call(method, params)
            return result[:1] if method == "debug_traceBlockByNumber" else result
        with self.assertRaises(CoverageError):
            index(self.db, start_block=10, call=incomplete)
        self.assertEqual(self.db.execute("SELECT count(*) FROM coverage").fetchone()[0], 0)
        self.assertEqual(self.db.execute("SELECT count(*) FROM deployments").fetchone()[0], 0)

    def test_reorg_anchor_stops_resume(self):
        index(self.db, start_block=10, call=fixture_call)
        def reorg(method, params=None):
            if method == "eth_getBlockByNumber" and params[1] is False:
                return {"hash": PARENT_HASH}
            return fixture_call(method, params)
        with self.assertRaises(CoverageError):
            index(self.db, call=reorg)

    def test_missing_persisted_height_stops_resume(self):
        index(self.db, start_block=10, call=fixture_call)
        with self.db:
            self.db.execute("DELETE FROM coverage WHERE height=10")
            self.db.execute("INSERT INTO coverage VALUES (11, ?, ?, 0, 0, 'now')",
                            (BLOCK_HASH, PARENT_HASH))
        with self.assertRaises(CoverageError):
            index(self.db, call=fixture_call)

    def test_direct_receipt_mismatch_fails(self):
        def bad_receipt(method, params=None):
            result = fixture_call(method, params)
            if method == "eth_getTransactionReceipt":
                return {**result, "contractAddress": FACTORY}
            return result
        with self.assertRaises(CoverageError):
            index(self.db, start_block=10, call=bad_receipt)
        self.assertEqual(self.db.execute("SELECT count(*) FROM coverage").fetchone()[0], 0)

    def test_assessment_and_dedup_are_gated(self):
        index(self.db, start_block=10, call=fixture_call)
        candidate = {"project_id": "example", "address": DIRECT, "sources": [
            {"kind": kind, "contract_address": DIRECT, "url": "https://example.org/proof"}
            for kind in ("project", "product", "developer")],
            "metrics": {name: {"value": 1, "evidence_url": "https://example.org/metric",
                               "observed_at_utc": "2026-11-10T00:00:00Z"}
                        for name in ("product", "user_growth", "onchain_activity",
                                     "liquidity", "revenue", "risk")}}
        report = index(self.db, call=fixture_call)
        a = assess(candidate, self.db, report)
        self.assertEqual(a["score"], 0)
        self.assertEqual(a["matched_source_kinds"], [])
        for kind in ("project", "product", "developer"):
            record_source_review(self.db, project_id="example", contract_address=DIRECT,
                kind=kind, url="https://example.org/proof", snapshot=f"Contract {DIRECT}",
                reviewed_by="reviewer-1", reviewed_at_utc="2026-11-10T00:00:00Z")
        for name in ("product", "user_growth", "onchain_activity", "liquidity", "revenue", "risk"):
            record_metric_review(self.db, project_id="example", metric=name, value=1,
                evidence_url="https://example.org/metric", observed_at_utc="2026-11-10T00:00:00Z",
                methodology="Fixture: normalized maximum", reviewed_by="reviewer-1",
                reviewed_at_utc="2026-11-10T00:01:00Z")
        a = assess(candidate, self.db, report)
        self.assertEqual(a["score"], 100)
        self.assertFalse(a["eligible"])
        self.assertFalse(propose_alert(self.db, "example", a))
        # Even after an explicit gate opens, repeat proposals are idempotent.
        a["eligible"] = True
        a["reasons"] = []
        self.assertTrue(propose_alert(self.db, "example", a))
        self.assertFalse(propose_alert(self.db, "example", a))

    def test_source_review_rejects_unlinked_contract(self):
        with self.assertRaises(ValueError):
            record_source_review(self.db, project_id="example", contract_address=DIRECT,
                kind="project", url="https://example.org/proof", snapshot="No contract here",
                reviewed_by="reviewer-1", reviewed_at_utc="2026-11-10T00:00:00Z")


if __name__ == "__main__":
    unittest.main()
