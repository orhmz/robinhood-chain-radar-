# Blockscout free API feasibility probe

The [Blockscout probe workflow](../.github/workflows/blockscout-probe.yml) uses the `BLOCKSCOUT_API_KEY` repository secret. It makes read-only requests to the unified Blockscout API for Robinhood Chain 4663 and Arc 5042. The key is sent as an Authorization header, never placed in a URL, report, or commit.

It checks chain stats, a recent creation-filter page and its next page when present, and a previously observed internal contract-creation sample in Robinhood block 111009151. Missing authentication, malformed pages, cursor overlap, or the missing known sample makes the job fail. The report contains counts and period bounds, not candidate or token claims.

`status=source_capability_verified` means the API key and a bounded internal creation sample worked. The report always sets `chain_wide_discovery=false`, `historical_contiguity_verified=false`, `create2_verified=false`, and `alert_eligible=false`. It does not advance a production checkpoint or send notifications.

Before production early-project alerts, build a durable range scanner that exhausts every page for each nonoverlapping time interval, verifies source index freshness and block/receipt evidence, tests CREATE2 against independent traces, handles revisions and reorgs, attributes first-party projects, and proves sustained usage within the free-tier credits. A successful two-page probe is not that proof.
