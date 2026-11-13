import datetime as dt
import sqlite3
import unittest

from radar.blockscout_probe import SourceError
from radar.scoped_candidates import collect, source_freshness, verify_row


ADDRESS = "0x" + "a" * 40
TX = "0x" + "b" * 64


def metadata(chain_id, path, params, key):
    if path.startswith("addresses/"):
        return {"is_verified": True, "creation_transaction_hash": TX}
    return {"is_fully_verified": True}


def rpc(method, params=None):
    if method == "eth_chainId":
        return "0x1237"  # 4663
    if method == "eth_getTransactionReceipt":
        return {"status": "0x1", "blockNumber": "0x64", "contractAddress": ADDRESS}
    return "0x6000"


class ScopedCandidatesTests(unittest.TestCase):
    def test_direct_creation_has_independent_receipt_and_code(self):
        reason, item = verify_row((4663, TX, -1, ADDRESS, 100), key="test-only",
                                  fetcher=metadata, rpc=rpc)
        self.assertEqual(reason, "evidence_recorded")
        self.assertEqual(item["verification_level"], "direct_receipt_and_current_code")
        self.assertIsNone(item["opportunity_score"])
        self.assertFalse(item["alert_eligible"])

    def test_internal_creation_remains_pending_trace(self):
        def internal_rpc(method, params=None):
            value = rpc(method, params)
            if method == "eth_getTransactionReceipt":
                value["contractAddress"] = None
            return value

        reason, item = verify_row((4663, TX, 0, ADDRESS, 100), key="test-only",
                                  fetcher=metadata, rpc=internal_rpc)
        self.assertEqual(reason, "evidence_recorded")
        self.assertEqual(item["verification_level"],
                         "internal_link_from_blockscout_pending_trace")
        self.assertFalse(item["project_identity_verified"])

    def test_source_mismatch_rejects_candidate(self):
        def mismatched(chain_id, path, params, key):
            value = metadata(chain_id, path, params, key)
            if path.startswith("addresses/"):
                value["creation_transaction_hash"] = "0x" + "c" * 64
            return value

        reason, item = verify_row((4663, TX, -1, ADDRESS, 100), key="test-only",
                                  fetcher=mismatched, rpc=rpc)
        self.assertEqual(reason, "creation_link_mismatch")
        self.assertIsNone(item)

    def test_chain_mismatch_fails_closed(self):
        db = sqlite3.connect(":memory:")
        db.execute("""CREATE TABLE source_creations (chain_id INTEGER,
            transaction_hash TEXT, internal_index INTEGER, address TEXT,
            block_number INTEGER)""")
        with self.assertRaises(SourceError):
            collect(db, "test-only", fetcher=metadata,
                    rpc_by_chain={4663: lambda *args: "0x1", 5042: rpc})

    def test_bounded_selection_and_no_identity_claims(self):
        db = sqlite3.connect(":memory:")
        db.execute("""CREATE TABLE source_creations (chain_id INTEGER,
            transaction_hash TEXT, internal_index INTEGER, address TEXT,
            block_number INTEGER)""")
        db.execute("INSERT INTO source_creations VALUES (?, ?, ?, ?, ?)",
                   (4663, TX, -1, ADDRESS, 100))
        def arc_rpc(method, params=None):
            return "0x13b2" if method == "eth_chainId" else rpc(method, params)
        report = collect(db, "test-only", fetcher=metadata,
                         rpc_by_chain={4663: rpc, 5042: arc_rpc}, limit=1)
        self.assertEqual(report["chains"]["4663"]["evidence_recorded"], 1)
        self.assertEqual(report["chains"]["5042"]["selected"], 0)
        self.assertFalse(report["chain_wide_coverage"])
        self.assertFalse(report["project_identity_verified"])
        self.assertFalse(report["alert_eligible"])

    def test_stale_source_is_detected(self):
        db = sqlite3.connect(":memory:")
        db.execute("CREATE TABLE streams (chain_id INTEGER, next_start_utc TEXT)")
        for chain in (4663, 5042):
            db.execute("INSERT INTO streams VALUES (?, ?)",
                       (chain, "2026-11-13T12:00:00Z"))
        now = dt.datetime(2026, 11, 13, 17, tzinfo=dt.timezone.utc)
        fresh, cutoffs = source_freshness(db, now)
        self.assertFalse(fresh)
        self.assertEqual(set(cutoffs), {"4663", "5042"})


if __name__ == "__main__":
    unittest.main()
