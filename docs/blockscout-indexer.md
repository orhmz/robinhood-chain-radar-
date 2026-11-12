# Durable Blockscout source indexer (opt-in)

`radar.blockscout_indexer` uses the free unified Blockscout API to collect
`CONTRACT_CREATION` records for Robinhood 4663 or Arc 5042. It does not change
the existing scheduled radar, RPC checkpoint, assessment ledger, or alerts.

## Coverage rules

- Bootstrap time is explicit and persisted per chain. Each fixed UTC interval
  shares its boundary with the next interval, so a creation at a boundary is
  fetched twice and deduplicated by transaction hash, internal index, and
  created address.
- The worker fetches **every** cursor page in an interval. A malformed response,
  repeated cursor, duplicate item, missing created address, unexpected type,
  page-budget exhaustion, or HTTP failure stops the run. The interval and its
  checkpoint are committed together only after the final page.
- The filter can return failed creation attempts, including status text such
  as `execution reverted`, sometimes with an attempted contract address.
  Explicitly recognized failure statuses are skipped regardless of address;
  unknown statuses fail closed. Only successful creations enter the index.
- On restart it checks persisted interval adjacency and replays the most recent
  completed intervals. Changed source rows stop the worker before new coverage
  is committed. A 30-minute lag avoids the newest explorer data. Neither this
  lag nor a successful page response proves that Blockscout has finished
  indexing the underlying chain.
- The report separates `source_windows_contiguous_from_bootstrap` from
  `chain_wide_coverage`. The latter and `create2_verified` and
  `alert_eligible` remain false. The API does not identify CREATE versus
  CREATE2 in these records, and its completeness has not been independently
  reconciled with traces.

Use a persistent SQLite file and a single writer. A GitHub Actions runner has
ephemeral storage, so the PR workflow only tests two bounded historical
intervals and discards its database. It never schedules production indexing.

## Run with durable storage

Set `BLOCKSCOUT_API_KEY` as an environment secret, then:

```sh
python -m radar.blockscout_indexer \
  --database /durable/blockscout.sqlite \
  --chain-id 4663 \
  --bootstrap-utc 2026-11-10T16:08:20Z \
  --window-seconds 300 --max-windows 12 \
  --report /durable/robinhood-source-report.json
```

Repeat the command to advance another batch. Use a separate invocation for
chain 5042. Bootstrap at a chosen date creates a declared historical gap before
that date. The default client spaces requests by at least 0.25 seconds and
retries only bounded 429/5xx failures. If a five-minute interval exceeds 1,000
pages, it fails visibly; choose a smaller fixed interval in a new database
instead of treating the partial result as complete. Monitor the report's
`unscanned_safe_seconds` and API credit consumption against the free tier.

Before alerts, verify the API's source freshness, reconcile sampled direct and
factory deployments against independent receipts and traces, establish CREATE2
coverage, attribute projects from first-party evidence, and run a durable
checkpoint and outbox service. The existing offline 100-point assessment
remains the only scoring path.

The large-interval PR test initially encountered one Blockscout timeout.
Timeouts now receive bounded retries; exhaustion still fails without advancing
the checkpoint. A repeated timeout is a source-capacity problem, not proof of
an empty interval.
