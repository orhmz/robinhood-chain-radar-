"""Trace-worker RPC transport; keeps endpoint credentials out of diagnostics."""
import base64
import json
import os
import time
import urllib.error
import urllib.request

DEFAULT_RPC = "https://rpc.mainnet.chain.robinhood.com"


class RpcError(RuntimeError):
    pass


def call(method, params=None, opener=urllib.request.urlopen, sleeper=time.sleep):
    url = os.getenv("ROBINHOOD_RPC_URL", DEFAULT_RPC)
    bearer = os.getenv("ROBINHOOD_RPC_BEARER_TOKEN")
    x_token = os.getenv("ROBINHOOD_RPC_X_TOKEN")
    basic_user = os.getenv("ROBINHOOD_RPC_BASIC_USER")
    basic_password = os.getenv("ROBINHOOD_RPC_BASIC_PASSWORD")
    if bool(basic_user) != bool(basic_password):
        raise RpcError("Both RPC basic username and password are required")
    if sum(bool(value) for value in (bearer, x_token, basic_user)) > 1:
        raise RpcError("Configure only one RPC authentication method")
    headers = {"Content-Type": "application/json", "Accept": "application/json",
               "User-Agent": "Robinhood-Chain-Data-Health/1.1"}
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    if x_token:
        headers["x-token"] = x_token
    if basic_user:
        credentials = base64.b64encode(f"{basic_user}:{basic_password}".encode()).decode()
        headers["Authorization"] = f"Basic {credentials}"
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                       "params": params or []}).encode()
    request = urllib.request.Request(url, data=body, headers=headers)
    for attempt in range(3):
        try:
            with opener(request, timeout=25) as response:
                payload = json.load(response)
            break
        except urllib.error.HTTPError as exc:
            if exc.code in (429, 503) and attempt < 2:
                retry_after = exc.headers.get("Retry-After") if exc.headers else None
                try:
                    delay = min(10, max(0, float(retry_after))) if retry_after else 2 ** attempt
                except ValueError:
                    delay = 2 ** attempt
                sleeper(delay)
                continue
            raise RpcError(f"{method}: HTTP {exc.code}") from None
        except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
            raise RpcError(f"{method}: transport or JSON error ({type(exc).__name__})") from None
    if not isinstance(payload, dict):
        raise RpcError(f"{method}: invalid RPC response")
    if payload.get("error"):
        error = payload["error"]
        if not isinstance(error, dict):
            raise RpcError(f"{method}: invalid RPC error response")
        message = str(error.get("message", ""))
        for secret in (url, bearer, x_token, basic_user, basic_password):
            if secret:
                message = message.replace(secret, "[redacted]")
        raise RpcError(f"{method}: RPC error {error.get('code')}: {message}")
    if payload.get("result") is None:
        raise RpcError(f"{method}: missing RPC result")
    return payload["result"]
