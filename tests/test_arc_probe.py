import unittest

from radar.arc_probe import (BOUNTY_ADAPTER, CHAIN_ID, KNOWN_CONTRACTS,
                             KNOWN_CREATION_BLOCK, KNOWN_CREATION_TX, collect)


def fake_rpc(method, params=None):
    if method == "eth_chainId":
        return hex(CHAIN_ID)
    if method == "eth_blockNumber":
        return hex(KNOWN_CREATION_BLOCK + 200)
    if method == "eth_getBlockByNumber":
        height = int(params[0], 16)
        return {"number": hex(height), "hash": "0xabc",
                "transactions": [KNOWN_CREATION_TX] if height == KNOWN_CREATION_BLOCK else []}
    if method == "eth_getCode":
        return "0x6000"
    if method == "eth_getTransactionReceipt":
        return {"status": "0x1", "contractAddress": BOUNTY_ADAPTER,
                "blockNumber": hex(KNOWN_CREATION_BLOCK)}
    if method == "eth_getLogs":
        return []
    raise AssertionError(f"Unexpected method {method}")


class ArcProbeTests(unittest.TestCase):
    def test_bounded_known_contract_evidence_never_claims_full_discovery(self):
        result = collect(fake_rpc)
        self.assertEqual(result["status"], "known_contract_code_verified")
        self.assertEqual(len(result["known_contracts"]), len(KNOWN_CONTRACTS))
        self.assertTrue(result["known_creation_receipt_verified"])
        self.assertEqual(result["known_address_log_window"]["to_block"],
                         result["last_checked_block"])
        self.assertFalse(result["coverage"]["chain_wide_discovery"])
        self.assertFalse(result["coverage"]["alert_eligible"])

    def test_missing_historical_receipt_remains_unverified(self):
        def no_receipt(method, params=None):
            return None if method == "eth_getTransactionReceipt" else fake_rpc(method, params)

        result = collect(no_receipt)
        self.assertEqual(result["status"], "known_contract_code_verified")
        self.assertFalse(result["known_creation_receipt_verified"])
        self.assertIn("not verified", result["known_creation_receipt_note"])

    def test_wrong_chain_or_missing_code_fails_closed(self):
        def wrong_chain(method, params=None):
            return "0x1" if method == "eth_chainId" else fake_rpc(method, params)

        self.assertEqual(collect(wrong_chain)["status"], "incomplete")

        def no_code(method, params=None):
            return "0x" if method == "eth_getCode" else fake_rpc(method, params)

        result = collect(no_code)
        self.assertEqual(result["status"], "incomplete")
        self.assertFalse(result["coverage"]["alert_eligible"])

    def test_unavailable_known_address_logs_fail_closed(self):
        def no_logs(method, params=None):
            return None if method == "eth_getLogs" else fake_rpc(method, params)

        self.assertEqual(collect(no_logs)["status"], "incomplete")


if __name__ == "__main__":
    unittest.main()
