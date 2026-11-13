import json
import sqlite3
import unittest

from radar.blockscout_probe import SourceError
from radar.verified_source_probe import probe


class VerifiedSourceProbeTests(unittest.TestCase):
    def test_reports_capability_without_exposing_candidate_address(self):
        db = sqlite3.connect(":memory:")
        db.execute("""CREATE TABLE source_creations
            (chain_id INTEGER, address TEXT, block_number INTEGER)""")
        address = "0x" + "a" * 40
        for chain in (4663, 5042):
            db.execute("INSERT INTO source_creations VALUES (?, ?, 100)",
                       (chain, address))

        def fetcher(chain, path, params, key):
            if path == "smart-contracts":
                self.assertEqual(params, {})
                return {"items": [{"address": {"hash": address}}],
                        "next_page_params": None}
            if path.startswith("smart-contracts/"):
                raise SourceError(f"Blockscout chain {chain} {path}: HTTP 404")
            return {"is_verified": False, "hash": address}

        report = probe(db, "test-only", fetcher)
        self.assertEqual(report["status"], "metadata_api_capability_observed")
        self.assertFalse(report["alert_eligible"])
        self.assertNotIn(address, json.dumps(report))
        self.assertNotIn("test-only", json.dumps(report))

    def test_access_failure_is_incomplete(self):
        db = sqlite3.connect(":memory:")
        db.execute("""CREATE TABLE source_creations
            (chain_id INTEGER, address TEXT, block_number INTEGER)""")

        def forbidden(*args):
            raise SourceError("HTTP 403")

        report = probe(db, "test-only", forbidden)
        self.assertEqual(report["status"], "incomplete")
        self.assertFalse(report["project_identity_verified"])


if __name__ == "__main__":
    unittest.main()
