# Rolling free source service

The scheduled `Rolling Blockscout source coverage (no alerts)` workflow
ingests creation-filter pages for Robinhood 4663 and Arc 5042 every two hours.
It uses fixed five-minute windows and a 30-minute lag. Every page in a window
must complete before that window's checkpoint advances.

## Persistence and failure behavior

GitHub Actions runners have no persistent disk. Each successful run uploads a
SQLite artifact and commits a small reference containing its artifact ID,
SHA-256 digest, per-chain bootstrap and next interval. The next run restores
**that exact artifact**, checks its digest, SQLite integrity, interval
continuity and checkpoint fields, then resumes. Missing, expired, tampered or
incompatible artifacts fail the run. They do not trigger a new bootstrap.

The artifact expires after two days. The database retains at most two days of
source windows to control free storage use. Old windows are explicitly dropped
from the reported scope. If GitHub skips runs long enough for the artifact to
expire, an operator must deliberately recover or rebootstrap with a declared
historical gap. GitHub artifacts are therefore a bounded staging store, not a
permanent chain archive.

The initial run starts two hours before its execution time, rounded to a
five-minute boundary. This is an explicit historical gap. A run fails if either
chain remains a full five-minute interval behind its 30-minute safe cutoff
after the permitted batch. Source/API errors also fail, and no checkpoint
reference is published for that run. The old reference remains available for
retry.

The scheduled service and the existing data-health workflow share a
concurrency group so their checkpoint commits cannot overlap. The new service
does not change the old workflow, alert automations, RPC trace indexer, or
assessment ledger.

## What a successful run means

It means recent **Blockscout source pages** were exhausted and their stored
rows passed a recent replay check. It does not mean full block coverage,
CREATE2 completeness, first-party project attribution or alert readiness.
Every report and state file says `chain_wide_coverage=false` and
`alert_eligible=false`. The source index cannot activate live notifications.

The free API and GitHub artifact quotas must be watched. A source timeout,
replay drift, artifact expiry, missing page or storage limit causes a visible
failure. A one-hour API request repeatedly timed out in development; the
five-minute windows passed real API tests.
