import datetime as dt
from pathlib import Path
import tempfile
import unittest

from radar.blockscout_indexer import (
    Client, SourceError, check_contiguous, open_store, prune_windows, read_window, run,
)


START = dt.datetime(2026, 11, 10, 16, 8, 20, tzinfo=dt.timezone.utc)
NOW = dt.datetime(2026, 11, 10, 17, 0, tzinfo=dt.timezone.utc)


def item(number, timestamp="2026-11-10T16:08:25Z", internal=2):
    return {
        "type": "contract_creation", "status": "success",
        "hash": "0x" + f"{number:064x}",
        "created_contract": {"hash": "0x" + f"{number:040x}"},
        "block_number": 111009151,
        "internal_transaction_index": internal,
        "timestamp": timestamp,
    }


class Fake:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def __call__(self, chain, path, params, key):
        self.calls.append((chain, path, params.copy(), key))
        return self.pages[len(self.calls) - 1]


class BlockscoutIndexerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = open_store(Path(self.temp.name) / "source.sqlite")

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def client(self, pages):
        fake = Fake(pages)
        return Client("test-only", fake, min_interval=0), fake

    def test_transient_remote_disconnect_retries_within_budget(self):
        calls = []

        def fetcher(chain, path, params, key):
            calls.append(path)
            if len(calls) < 3:
                raise SourceError("Blockscout advanced-filters: transport or JSON error "
                                  "(RemoteDisconnected)")
            return {"items": [], "next_page_params": None}

        client = Client("test-only", fetcher, min_interval=0,
                        sleeper=lambda _: None)
        self.assertEqual(client.get(4663, {}),
                         {"items": [], "next_page_params": None})
        self.assertEqual(len(calls), 3)

    def test_exhausts_cursor_and_records_factory_and_direct(self):
        first = {"items": [item(1)], "next_page_params": {"block_number": 10}}
        second = {"items": [item(2, internal=None)], "next_page_params": None}
        client, fake = self.client([first, second])
        rows, pages, digest = read_window(
            client, 4663, START, START + dt.timedelta(seconds=10))
        self.assertEqual((len(rows), pages, len(digest)), (2, 2, 64))
        self.assertEqual(rows[1][1], -1)
        self.assertEqual(fake.calls[1][2]["block_number"], 10)
        self.assertEqual(fake.calls[1][2]["age_from"], "2026-11-10T16:08:20Z")

    def test_page_limit_fails_without_checkpoint(self):
        client, _ = self.client([
            {"items": [item(1)], "next_page_params": {"block_number": 10}},
        ])
        with self.assertRaisesRegex(SourceError, "Page budget"):
            run(self.db, client, chain_id=4663, bootstrap=START, window_seconds=10,
                max_windows=1, max_pages=1, now=NOW)
        self.assertEqual(self.db.execute("SELECT count(*) FROM source_windows").fetchone()[0], 0)
        self.assertEqual(self.db.execute("SELECT next_start_utc FROM streams").fetchone()[0],
                         "2026-11-10T16:08:20Z")

    def test_repeated_cursor_and_duplicate_page_fail(self):
        client, _ = self.client([
            {"items": [item(1)], "next_page_params": {"cursor": "a"}},
            {"items": [item(2)], "next_page_params": {"cursor": "a"}},
        ])
        with self.assertRaisesRegex(SourceError, "Repeated pagination cursor"):
            read_window(client, 5042, START, START + dt.timedelta(seconds=10))
        client, _ = self.client([
            {"items": [item(1)], "next_page_params": {"cursor": "a"}},
            {"items": [item(1)], "next_page_params": None},
        ])
        with self.assertRaisesRegex(SourceError, "Duplicate creation"):
            read_window(client, 4663, START, START + dt.timedelta(seconds=10))

    def test_bad_item_or_cursor_cannot_advance(self):
        bad = item(1)
        bad["created_contract"] = None
        for page in [
            {"items": [bad], "next_page_params": None},
            {"items": [item(1)], "next_page_params": {"age_from": "other"}},
            {"items": [item(1, timestamp="2026-11-10T16:09:25Z")],
             "next_page_params": None},
        ]:
            with self.subTest(page=page):
                client, _ = self.client([page])
                with self.assertRaises(SourceError):
                    read_window(client, 4663, START, START + dt.timedelta(seconds=10))

    def test_failed_creation_attempt_is_not_a_deployment(self):
        failed = item(1)
        failed["status"] = "execution reverted"
        # Blockscout may still expose an attempted address on a reverted call.
        client, _ = self.client([
            {"items": [failed, item(2)], "next_page_params": None}
        ])
        rows, pages, _ = read_window(
            client, 4663, START, START + dt.timedelta(seconds=10))
        self.assertEqual((len(rows), pages), (1, 1))
        unknown = item(3)
        unknown["status"] = "contract creation code storage out of gas"
        client, _ = self.client([
            {"items": [unknown], "next_page_params": None}
        ])
        rows, _, _ = read_window(client, 4663, START, START + dt.timedelta(seconds=10))
        self.assertEqual(rows, [])
        malformed = item(4)
        malformed["status"] = ""
        client, _ = self.client([
            {"items": [malformed], "next_page_params": None}
        ])
        with self.assertRaisesRegex(SourceError, "lacks an execution status"):
            read_window(client, 4663, START, START + dt.timedelta(seconds=10))

    def test_contiguous_windows_dedupe_shared_boundary(self):
        page = {"items": [item(1, timestamp="2026-11-10T16:08:25Z")],
                "next_page_params": None}
        client, _ = self.client([page, page])
        result = run(self.db, client, chain_id=4663, bootstrap=START,
                     window_seconds=5, max_windows=2, now=NOW)
        self.assertEqual(result["windows_committed"], 2)
        self.assertEqual(result["next_start_utc"], "2026-11-10T16:08:30Z")
        self.assertFalse(result["chain_wide_coverage"])
        self.assertFalse(result["alert_eligible"])
        self.assertEqual(self.db.execute("SELECT count(*) FROM source_windows").fetchone()[0], 2)
        self.assertEqual(self.db.execute("SELECT count(*) FROM source_creations").fetchone()[0], 1)
        check_contiguous(self.db, 4663, START, START + dt.timedelta(seconds=10), 5)

    def test_recheck_detects_source_drift_before_advancing(self):
        page = {"items": [item(1)], "next_page_params": None}
        first, _ = self.client([page])
        run(self.db, first, chain_id=4663, bootstrap=START, window_seconds=10,
            max_windows=1, now=NOW)
        changed, _ = self.client([
            {"items": [item(2)], "next_page_params": None}
        ])
        with self.assertRaisesRegex(SourceError, "Indexed source changed"):
            run(self.db, changed, chain_id=4663, bootstrap=START,
                window_seconds=10, max_windows=1, now=NOW)
        self.assertEqual(self.db.execute("SELECT next_start_utc FROM streams").fetchone()[0],
                         "2026-11-10T16:08:30Z")

    def test_verify_only_replays_without_advancing(self):
        page = {"items": [item(1)], "next_page_params": None}
        first, _ = self.client([page])
        run(self.db, first, chain_id=4663, bootstrap=START, window_seconds=10,
            max_windows=1, now=NOW)
        second, fake = self.client([page])
        report = run(self.db, second, chain_id=4663, bootstrap=START,
                     window_seconds=10, max_windows=0, now=NOW)
        self.assertEqual((report["windows_committed"], len(fake.calls)), (0, 1))
        self.assertEqual((report["rechecked_windows"], report["recheck_pages_fetched"]), (1, 1))
        self.assertEqual(report["next_start_utc"], "2026-11-10T16:08:30Z")

    def test_missing_stored_creation_is_not_silently_rechecked(self):
        page = {"items": [item(1)], "next_page_params": None}
        first, _ = self.client([page])
        run(self.db, first, chain_id=4663, bootstrap=START, window_seconds=10,
            max_windows=1, now=NOW)
        self.db.execute("DELETE FROM source_window_creations")
        self.db.commit()
        second, fake = self.client([])
        with self.assertRaisesRegex(SourceError, "Stored creations disagree"):
            run(self.db, second, chain_id=4663, bootstrap=START,
                window_seconds=10, max_windows=0, now=NOW)
        self.assertEqual(fake.calls, [])

    def test_rolling_prune_retains_adjacent_windows_and_shared_boundary(self):
        boundary = item(1, timestamp="2026-11-10T16:08:25Z")
        later = item(2, timestamp="2026-11-10T16:08:35Z")
        first, _ = self.client([
            {"items": [boundary], "next_page_params": None},
            {"items": [boundary], "next_page_params": None},
            {"items": [], "next_page_params": None},
            {"items": [later], "next_page_params": None},
        ])
        run(self.db, first, chain_id=4663, bootstrap=START, window_seconds=5,
            max_windows=4, now=NOW)
        self.assertEqual(prune_windows(self.db, 4663, 2),
                         "2026-11-10T16:08:30Z")
        self.assertEqual(self.db.execute(
            "SELECT count(*) FROM source_windows").fetchone()[0], 2)
        self.assertEqual(self.db.execute(
            "SELECT count(*) FROM source_creations").fetchone()[0], 1)
        check_contiguous(self.db, 4663, START + dt.timedelta(seconds=10),
                         START + dt.timedelta(seconds=20), 5)
        replay, _ = self.client([
            {"items": [later], "next_page_params": None},
            {"items": [], "next_page_params": None},
        ])
        result = run(self.db, replay, chain_id=4663,
                     bootstrap=START + dt.timedelta(seconds=10),
                     window_seconds=5, max_windows=0, now=NOW)
        self.assertEqual(result["rechecked_windows"], 2)

    def test_persisted_gap_is_rejected(self):
        first, _ = self.client([{"items": [], "next_page_params": None}])
        run(self.db, first, chain_id=5042, bootstrap=START, window_seconds=10,
            max_windows=1, now=NOW)
        self.db.execute("DELETE FROM source_windows")
        self.db.commit()
        second, fake = self.client([])
        with self.assertRaisesRegex(SourceError, "checkpoint disagrees"):
            run(self.db, second, chain_id=5042, bootstrap=START,
                window_seconds=10, max_windows=1, now=NOW)
        self.assertEqual(fake.calls, [])

    def test_retry_is_bounded_and_only_transient(self):
        calls = []
        def transient(chain, path, params, key):
            calls.append(chain)
            if len(calls) < 3:
                raise SourceError("HTTP 429")
            return {"items": [], "next_page_params": None}
        delays = []
        client = Client("test-only", transient, min_interval=0, sleeper=delays.append)
        self.assertEqual(client.get(4663, {}), {"items": [], "next_page_params": None})
        self.assertEqual((len(calls), delays), (3, [1, 2]))
        def forbidden(*args):
            raise SourceError("HTTP 403")
        client = Client("test-only", forbidden, min_interval=0, sleeper=delays.append)
        with self.assertRaisesRegex(SourceError, "HTTP 403"):
            client.get(4663, {})
        self.assertEqual(delays, [1, 2])
        calls.clear()
        def timeout_then_success(chain, path, params, key):
            calls.append(chain)
            if len(calls) == 1:
                raise SourceError("transport or JSON error (TimeoutError)")
            return {"items": [], "next_page_params": None}
        client = Client("test-only", timeout_then_success, min_interval=0,
                        sleeper=delays.append)
        client.get(4663, {})
        self.assertEqual((len(calls), delays[-1]), (2, 1))


if __name__ == "__main__":
    unittest.main()
