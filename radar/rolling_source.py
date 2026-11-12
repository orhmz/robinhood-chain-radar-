"""Scoped rolling Blockscout source ingestion for GitHub Actions.

Artifacts preserve a bounded SQLite database between scheduled runs. This is
not a chain-wide deployment index and it never sends notifications.
"""

import argparse
from contextlib import closing
import datetime as dt
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import urllib.error
import urllib.parse
import urllib.request
import zipfile

from radar.blockscout_indexer import (
    CHAINS, Client, SourceError, UTC, check_contiguous, open_store,
    parse_time, prune_windows, run, stamp,
)


WINDOW_SECONDS = 300
RETAIN_WINDOWS = 576  # 48 hours. A rolling source scope, not chain history.
MAX_ARTIFACT_BYTES = 64 * 1024 * 1024
DEFAULT_STATE = Path("blockscout_service_state.json")
DEFAULT_DATABASE = Path("out/rolling-blockscout.sqlite")
DEFAULT_REPORT = Path("out/rolling-source-report.json")


def load_state(path):
    if not path.exists():
        raise SourceError("Rolling source state file is missing; refusing implicit reset")
    state = json.loads(path.read_text())
    if not isinstance(state, dict) or state.get("version") != 1 or (
            type(state.get("initialized")) is not bool):
        raise SourceError("Malformed rolling source state")
    if state["initialized"]:
        if type(state.get("artifact_id")) is not int or state["artifact_id"] < 1 or (
                type(state.get("artifact_run_id")) is not int or
                state["artifact_run_id"] < 1):
            raise SourceError("Rolling source artifact reference is missing")
        if not isinstance(state.get("db_sha256"), str) or len(state["db_sha256"]) != 64:
            raise SourceError("Rolling source database digest is missing")
        chains = state.get("chains")
        if not isinstance(chains, dict) or set(chains) != {str(x) for x in CHAINS}:
            raise SourceError("Rolling source chain checkpoints are missing")
        for value in chains.values():
            if not isinstance(value, dict) or not all(
                    key in value for key in ("bootstrap_utc", "next_start_utc")):
                raise SourceError("Malformed rolling source chain checkpoint")
            if parse_time(value["bootstrap_utc"]) > parse_time(value["next_start_utc"]):
                raise SourceError("Rolling source checkpoint precedes bootstrap")
    return state


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def verify_database(path, state):
    if not path.exists():
        raise SourceError("Rolling source database is missing")
    with closing(sqlite3.connect(path)) as db:
        if db.execute("PRAGMA integrity_check").fetchone() != ("ok",):
            raise SourceError("Rolling source SQLite integrity check failed")
        for chain_id in CHAINS:
            expected = state["chains"][str(chain_id)]
            row = db.execute("""SELECT bootstrap_utc, next_start_utc, window_seconds
                FROM streams WHERE chain_id=?""", (chain_id,)).fetchone()
            if row != (expected["bootstrap_utc"], expected["next_start_utc"],
                       WINDOW_SECONDS):
                raise SourceError(f"Rolling source checkpoint mismatch on {chain_id}")
            check_contiguous(db, chain_id, parse_time(row[0]), parse_time(row[1]),
                             WINDOW_SECONDS)


class NoAuthRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None  # Read the signed Location and request it without GitHub token.


def download_artifact(repo, token, state):
    if not token or not repo or "/" not in repo:
        raise SourceError("GitHub artifact credentials or repository missing")
    root = f"https://api.github.com/repos/{repo}/actions/artifacts/{state['artifact_id']}"
    headers = {"Authorization": "Bearer " + token, "Accept": "application/vnd.github+json",
               "User-Agent": "rolling-blockscout-source/1.0"}
    try:
        with urllib.request.urlopen(urllib.request.Request(root, headers=headers),
                                    timeout=20) as response:
            metadata = json.load(response)
        if metadata.get("expired") or metadata.get("name") != (
                f"blockscout-rolling-db-{state['artifact_run_id']}") or (
                metadata.get("workflow_run", {}).get("id") != state["artifact_run_id"]):
            raise SourceError("Referenced rolling source artifact does not match state")
        opener = urllib.request.build_opener(NoAuthRedirect)
        try:
            opener.open(urllib.request.Request(root + "/zip", headers=headers), timeout=20)
        except urllib.error.HTTPError as error:
            if error.code not in (301, 302, 303, 307, 308):
                raise
            location = error.headers.get("Location")
        else:
            raise SourceError("GitHub artifact download did not return a signed URL")
        parsed = urllib.parse.urlparse(location or "")
        if parsed.scheme != "https" or not parsed.hostname:
            raise SourceError("GitHub artifact redirect is not HTTPS")
        with urllib.request.urlopen(urllib.request.Request(location, headers={
                "User-Agent": "rolling-blockscout-source/1.0"}), timeout=60) as response:
            blob = response.read(MAX_ARTIFACT_BYTES + 1)
    except SourceError:
        raise
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError,
            ValueError, TypeError) as error:
        raise SourceError(f"Cannot restore rolling source artifact ({type(error).__name__})") from None
    if len(blob) > MAX_ARTIFACT_BYTES:
        raise SourceError("Rolling source artifact exceeds size limit")
    return blob


def restore_database(state, db_path, artifact_blob):
    if db_path.exists():
        raise SourceError("Rolling source database path already exists")
    try:
        with zipfile.ZipFile(io.BytesIO(artifact_blob)) as archive:
            if archive.namelist() != ["rolling-blockscout.sqlite"]:
                raise SourceError("Rolling source artifact contains unexpected files")
            member = archive.getinfo("rolling-blockscout.sqlite")
            if member.file_size > MAX_ARTIFACT_BYTES:
                raise SourceError("Rolling source database exceeds size limit")
            data = archive.read(member)
    except SourceError:
        raise
    except (zipfile.BadZipFile, RuntimeError, ValueError) as error:
        raise SourceError(f"Invalid rolling source artifact ({type(error).__name__})") from None
    if sha256(data) != state["db_sha256"]:
        raise SourceError("Rolling source artifact digest mismatch")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = db_path.with_suffix(".restore-tmp")
    temporary.write_bytes(data)
    try:
        verify_database(temporary, state)
        temporary.replace(db_path)
    finally:
        temporary.unlink(missing_ok=True)


def initial_bootstrap(now):
    seconds = int((now - dt.timedelta(hours=2)).timestamp())
    return dt.datetime.fromtimestamp(seconds - seconds % WINDOW_SECONDS, UTC)


def ingest(db_path, state, key, *, now=None, max_windows=288, retain_windows=RETAIN_WINDOWS):
    if not key:
        raise SourceError("BLOCKSCOUT_API_KEY is missing")
    now = now or dt.datetime.now(UTC)
    if max_windows < 1 or retain_windows < 12:
        raise ValueError("Invalid rolling source limits")
    client = Client(key)
    reports = {}
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(open_store(db_path)) as db:
        for chain_id in CHAINS:
            bootstrap = (parse_time(state["chains"][str(chain_id)]["bootstrap_utc"])
                         if state["initialized"] else initial_bootstrap(now))
            report = run(db, client, chain_id=chain_id, bootstrap=bootstrap,
                         window_seconds=WINDOW_SECONDS, max_windows=max_windows,
                         recheck_windows=12, now=now)
            if report["unscanned_safe_seconds"] >= WINDOW_SECONDS:
                raise SourceError(f"Source chain {chain_id} remains behind safe cutoff")
            retained_bootstrap = prune_windows(db, chain_id, retain_windows)
            report["bootstrap_utc"] = retained_bootstrap
            report["retained_window_count"] = db.execute(
                "SELECT count(*) FROM source_windows WHERE chain_id=?",
                (chain_id,)).fetchone()[0]
            reports[str(chain_id)] = report
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        db.execute("VACUUM")
        db.execute("PRAGMA journal_mode=DELETE")
    if db_path.stat().st_size > MAX_ARTIFACT_BYTES:
        raise SourceError("Rolling source database exceeds artifact size limit")
    return {"status": "caught_up_source_only", "checked_at_utc": stamp(now),
            "chains": reports, "chain_wide_coverage": False,
            "create2_verified": False, "alert_eligible": False,
            "storage": "rolling_GitHub_artifact_with_expiry"}


def next_state(report, db_path, artifact_id, artifact_run_id):
    if report.get("status") != "caught_up_source_only" or any(
            type(value) is not int or value < 1
            for value in (artifact_id, artifact_run_id)):
        raise SourceError("Cannot finalize incomplete rolling source run")
    chains = {chain: {"bootstrap_utc": value["bootstrap_utc"],
                      "next_start_utc": value["next_start_utc"]}
              for chain, value in report["chains"].items()}
    state = {"version": 1, "initialized": True,
             "artifact_id": artifact_id, "artifact_run_id": artifact_run_id,
             "db_sha256": sha256(db_path.read_bytes()),
             "updated_at_utc": report["checked_at_utc"],
             "chains": chains, "chain_wide_coverage": False,
             "alert_eligible": False}
    # Check the exact standalone bytes that the artifact step uploads.
    standalone = db_path.with_suffix(".standalone-check")
    standalone.write_bytes(db_path.read_bytes())
    try:
        verify_database(standalone, state)
    finally:
        standalone.unlink(missing_ok=True)
    return state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("restore", "scan", "finalize"))
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--artifact-id", type=int)
    parser.add_argument("--max-windows", type=int, default=288)
    args = parser.parse_args()
    try:
        state = load_state(args.state)
        if args.command == "restore":
            if state["initialized"]:
                blob = download_artifact(os.getenv("GITHUB_REPOSITORY"),
                                         os.getenv("GITHUB_TOKEN"), state)
                restore_database(state, args.database, blob)
                print(json.dumps({"status": "restored_verified_source_checkpoint"}))
            elif args.database.exists():
                raise SourceError("Uninitialized state has an existing database")
            else:
                print(json.dumps({"status": "initial_source_bootstrap_required"}))
        elif args.command == "scan":
            if state["initialized"]:
                verify_database(args.database, state)
            report = ingest(args.database, state, os.getenv("BLOCKSCOUT_API_KEY"),
                            max_windows=args.max_windows)
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, indent=2) + "\n")
            print(json.dumps(report))
        else:
            report = json.loads(args.report.read_text())
            updated = next_state(report, args.database, args.artifact_id,
                                 int(os.getenv("GITHUB_RUN_ID", "0")))
            args.state.write_text(json.dumps(updated, indent=2) + "\n")
            print(json.dumps({"status": "source_checkpoint_ready",
                              "artifact_id": updated["artifact_id"]}))
    except (SourceError, sqlite3.Error, ValueError, KeyError, OSError) as error:
        failure = {"status": "incomplete", "error": str(error),
                   "chain_wide_coverage": False, "alert_eligible": False}
        if args.command == "scan":
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(failure, indent=2) + "\n")
        print(json.dumps(failure))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
