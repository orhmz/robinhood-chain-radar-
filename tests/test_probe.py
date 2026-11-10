import unittest

from radar.probe import probe


class ProbeTests(unittest.TestCase):
    def test_unsupported_trace_is_explicit(self):
        def rpc(method, params=None):
            if method == "eth_chainId":
                return hex(4663)
            if method == "eth_blockNumber":
                return hex(16)
            if method == "eth_getBlockByNumber":
                return {"number": hex(10), "transactions": ["0xabc"]}
            raise RuntimeError("debug_traceBlockByNumber: RPC error -32601: unavailable")
        report = probe(10, rpc)
        self.assertFalse(report["trace_supported"])
        self.assertIn("-32601", report["error"])

    def test_complete_trace_passes(self):
        def rpc(method, params=None):
            if method == "eth_chainId":
                return hex(4663)
            if method == "eth_blockNumber":
                return hex(16)
            if method == "eth_getBlockByNumber":
                return {"number": hex(10), "transactions": ["0xabc"]}
            return [{"txHash": "0xabc", "result": {"type": "CALL"}}]
        report = probe(10, rpc)
        self.assertTrue(report["trace_supported"])
        self.assertEqual(report["transaction_count"], 1)


if __name__ == "__main__":
    unittest.main()
