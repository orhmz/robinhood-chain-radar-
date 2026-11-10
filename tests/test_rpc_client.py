import io
import json
import unittest
import urllib.error
from unittest.mock import patch

from radar.rpc_client import RpcError, call


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class RpcClientTests(unittest.TestCase):
    def test_bearer_token_sent_in_header(self):
        def opener(request, timeout):
            self.assertEqual(request.get_header("Authorization"), "Bearer private-token")
            self.assertEqual(timeout, 25)
            return Response(b'{"jsonrpc":"2.0","id":1,"result":"0x1237"}')
        with patch.dict("os.environ", {"ROBINHOOD_RPC_URL": "https://example.org/rpc",
                                    "ROBINHOOD_RPC_BEARER_TOKEN": "private-token"}, clear=True):
            self.assertEqual(call("eth_chainId", opener=opener), "0x1237")

    def test_x_token_sent_and_error_redacted(self):
        def opener(request, timeout):
            self.assertEqual(request.get_header("X-token"), "private-token")
            return Response(json.dumps({"error": {"code": -32601,
                "message": "private-token not supported"}}).encode())
        with patch.dict("os.environ", {"ROBINHOOD_RPC_URL": "https://example.org/rpc",
                                    "ROBINHOOD_RPC_X_TOKEN": "private-token"}, clear=True):
            with self.assertRaises(RpcError) as caught:
                call("debug_traceBlockByNumber", opener=opener)
        self.assertNotIn("private-token", str(caught.exception))
        self.assertIn("-32601", str(caught.exception))

    def test_rate_limit_retries_without_claiming_success(self):
        attempts = 0
        delays = []
        def opener(request, timeout):
            nonlocal attempts
            attempts += 1
            raise urllib.error.HTTPError(request.full_url, 429, "limited", {}, None)
        with self.assertRaises(RpcError) as caught:
            call("eth_chainId", opener=opener, sleeper=delays.append)
        self.assertEqual(attempts, 3)
        self.assertEqual(delays, [1, 2])
        self.assertIn("HTTP 429", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
