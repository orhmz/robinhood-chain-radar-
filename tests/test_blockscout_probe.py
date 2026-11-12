import unittest
from datetime import datetime, timezone

from radar.blockscout_probe import KNOWN_ROBINHOOD_BLOCK, probe


NOW = datetime(2026, 11, 12, 18, tzinfo=timezone.utc)


def item(tx, address, *, block=100, internal=None):
    return {"type": "contract_creation", "status": "success", "hash": tx,
            "block_number": block, "internal_transaction_index": internal,
            "created_contract": {"hash": address}}


def fake_fetch(chain_id, path, params, key):
    assert key == "test-key"
    if path == "stats":
        return {"total_blocks": "123"}
    if params["age_from"] == "2026-11-10T16:08:20Z":
        return {"items": [item("0xknown", "0xcontract",
                               block=KNOWN_ROBINHOOD_BLOCK, internal=2)],
                "next_page_params": None}
    if chain_id == 4663:
        if "block_number" in params:
            return {"items": [item("0xsecond", "0xsecondaddress", internal=3)],
                    "next_page_params": None}
        return {"items": [item("0xfirst", "0xfirstaddress")],
                "next_page_params": {"block_number": 99,
                                     "internal_transaction_index": None}}
    return {"items": [item("0xarc", "0xarcaddress", internal=1)],
            "next_page_params": None}


class BlockscoutProbeTests(unittest.TestCase):
    def test_two_chains_pagination_and_known_factory_are_only_capability_proof(self):
        result = probe("test-key", now=NOW, fetcher=fake_fetch)
        self.assertEqual(result["status"], "source_capability_verified")
        self.assertEqual(result["known_factory_sample"]["internal_creations"], 1)
        self.assertEqual(result["chains"]["robinhood"]["second_page_items"], 1)
        self.assertFalse(result["coverage"]["chain_wide_discovery"])
        self.assertFalse(result["coverage"]["create2_verified"])
        self.assertFalse(result["coverage"]["alert_eligible"])

    def test_missing_key_or_factory_never_succeeds(self):
        self.assertEqual(probe(None, now=NOW, fetcher=fake_fetch)["status"], "incomplete")

        def no_factory(chain_id, path, params, key):
            if params.get("age_from") == "2026-11-10T16:08:20Z":
                return {"items": [], "next_page_params": None}
            return fake_fetch(chain_id, path, params, key)

        result = probe("test-key", now=NOW, fetcher=no_factory)
        self.assertEqual(result["status"], "incomplete")
        self.assertIn("factory creation missing", result["errors"][0])

    def test_pagination_overlap_is_an_error(self):
        def overlap(chain_id, path, params, key):
            if chain_id == 4663 and "block_number" in params:
                return {"items": [item("0xfirst", "0xfirstaddress")],
                        "next_page_params": None}
            return fake_fetch(chain_id, path, params, key)

        self.assertEqual(probe("test-key", now=NOW, fetcher=overlap)["status"], "incomplete")


if __name__ == "__main__":
    unittest.main()
