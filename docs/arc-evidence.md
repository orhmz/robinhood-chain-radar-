# Arc public RPC evidence bridge

The [Arc evidence workflow](../.github/workflows/arc-evidence.yml) uses the official public `https://rpc.mainnet.arc.io` endpoint. It provides a machine-readable artifact for the Arc chat radar when that automation cannot resolve the RPC hostname. Run `python -m radar.arc_probe --output out/arc_probe.json` for the same read-only check locally.

The probe requires chain ID 5042, a block 64 confirmations behind the head, nonempty bytecode for three previously documented ARCBounty addresses, inclusion of the known BountyAdapter transaction in historical block 21153190, and a successful 101-block log query for the known BountyAdapter address. A creation receipt is checked when the RPC provides one; an unavailable receipt is explicitly recorded as unverified. The probe never treats transaction inclusion alone as creation proof.

`status=known_contract_code_verified` proves only this bounded set of checks. Its `coverage` always says `known_contracts_only=true`, `chain_wide_discovery=false`, `factory_create2_discovery=false`, and `alert_eligible=false`. A zero log count for the known address is not evidence that no Arc projects launched. Any failed required RPC check produces `status=incomplete` and a failing workflow, with the partial JSON retained when available.

This workflow does not need a paid RPC account. Complete new-project discovery still requires a source of creation/factory events, a durable contiguous checkpoint, transaction or receipt verification, and first-party project attribution. The Arc chat radar should use this artifact only as evidence for the specific known addresses and checked period.
