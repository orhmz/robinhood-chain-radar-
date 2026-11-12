import datetime as dt
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from radar.blockscout_indexer import SourceError
from radar.rolling_source import (
    ingest, load_state, next_state, restore_database, verify_database,
)


class EmptyClient:
    def get(self, chain_id, params):
        return {"items": [], "next_page_params": None}


class RollingSourceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.marker = self.root / "state.json"
        self.marker.write_text(json.dumps({"version": 1, "initialized": False}))
        self.database = self.root / "rolling-blockscout.sqlite"

    def tearDown(self):
        self.temp.cleanup()

    def test_initial_scan_and_exact_artifact_round_trip(self):
        state = load_state(self.marker)
        now = dt.datetime(2026, 11, 12, 20, 0, tzinfo=dt.timezone.utc)
        with patch("radar.rolling_source.Client", return_value=EmptyClient()):
            report = ingest(self.database, state, "test-only", now=now, max_windows=20)
        self.assertEqual(report["status"], "caught_up_source_only")
        self.assertEqual(set(report["chains"]), {"4663", "5042"})
        self.assertEqual(report["chains"]["4663"]["windows_committed"], 18)
        self.assertFalse(report["alert_eligible"])
        updated = next_state(report, self.database, 123, 456)
        self.marker.write_text(json.dumps(updated))
        self.assertEqual(load_state(self.marker)["artifact_id"], 123)
        with io.BytesIO() as buffer:
            with zipfile.ZipFile(buffer, "w") as archive:
                archive.writestr("rolling-blockscout.sqlite", self.database.read_bytes())
            blob = buffer.getvalue()
        restored = self.root / "restored" / "rolling-blockscout.sqlite"
        restore_database(updated, restored, blob)
        verify_database(restored, updated)
        self.assertEqual(restored.read_bytes(), self.database.read_bytes())

    def test_missing_marker_and_changed_artifact_fail_closed(self):
        with self.assertRaisesRegex(SourceError, "state file is missing"):
            load_state(self.root / "missing.json")
        state = {"version": 1, "initialized": True,
                 "artifact_id": 123, "artifact_run_id": 456,
                 "db_sha256": "0" * 64,
                 "chains": {str(x): {
                     "bootstrap_utc": "2026-11-12T18:00:00Z",
                     "next_start_utc": "2026-11-12T18:05:00Z"}
                     for x in (4663, 5042)}}
        with io.BytesIO() as buffer:
            with zipfile.ZipFile(buffer, "w") as archive:
                archive.writestr("rolling-blockscout.sqlite", b"tampered")
            blob = buffer.getvalue()
        with self.assertRaisesRegex(SourceError, "digest mismatch"):
            restore_database(state, self.database, blob)
        self.assertFalse(self.database.exists())

    def test_artifact_with_extra_file_is_rejected(self):
        state = {"db_sha256": "0" * 64}
        with io.BytesIO() as buffer:
            with zipfile.ZipFile(buffer, "w") as archive:
                archive.writestr("rolling-blockscout.sqlite", b"data")
                archive.writestr("unexpected.txt", b"data")
            blob = buffer.getvalue()
        with self.assertRaisesRegex(SourceError, "unexpected files"):
            restore_database(state, self.database, blob)


if __name__ == "__main__":
    unittest.main()
