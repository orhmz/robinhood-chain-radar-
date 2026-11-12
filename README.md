# Robinhood Chain Radar — Data Health v1

Independent monitoring for Robinhood Chain (chain ID 4663). This is **not** an investment-signal engine yet.

## Safety guarantees
- A failed Blockscout response produces a failed GitHub Actions run.
- The last observed indexed block is recorded separately from *verified scan coverage*.
- The verified-contract feed is only a discovery sample; it is **not** evidence of exhaustive deployment coverage.
- No token, holder, TVL or revenue claims and no investment alerts are generated from this stage.
- Every run uploads `out/status.json` with source URL, access time, data time, and coverage limitations.

## Run
GitHub Actions → **Robinhood data health and discovery** → **Run workflow**.

The workflow runs every six hours. If the explorer blocks requests, the job will fail with diagnostics rather than silently claiming success. The existing ChatGPT Robinhood automation and DeFiLlama radar are not modified.

## Next phase
Add independent RPC verification, paginated contract-creation and transaction/log indexing with persistent **coverage** checkpoints, official project identity checks, token economics and duplicate-safe alert generation. Do not connect investment notifications before coverage tests pass.

An opt-in trace-complete indexer prototype, offline assessment rubric, and tests now live in `radar/indexer.py`, `radar/assessment.py`, and `tests/`. See [the architecture and operating limits](docs/indexing.md). This does not change the scheduled data-health workflow or enable notifications.

The separate [rolling Blockscout source service](docs/rolling-source.md) keeps a
bounded, source-only creation index using the free API. Its GitHub artifact
checkpoint is verified on restore and expires after two days. It does not
establish full-chain coverage or enable alerts.
