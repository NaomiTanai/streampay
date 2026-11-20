# StreamPay Dual-Read Reconciliation Service

Dual-read reconciliation service that compares stream states from **Horizon** (classic-bridge effects) against **Soroban RPC** contract state, accounts for consistency lag windows, and emits structured logs and webhooks on state divergence to detect index corruption or RPC failure.

Integrates with the **Issue #2 indexer dashboard withdrawal panel** via webhook dispatch.

---

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Horizon       │     │   Soroban RPC   │     │  Reconciliation │
│   Reader        │     │   Reader        │     │   Engine        │
│   (effects)     │     │   (contract     │     │   (diff.js)     │
│                 │     │    state)       │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌────────────▼─────────────┐
                    │  Alerting Module         │
                    │  (Structured Logs +      │
                    │   Webhook Dispatch)      │
                    └────────────┬─────────────┘
                                 │
                    ┌────────────▼─────────────┐
                    │  Indexer Dashboard       │
                    │  (Issue #2 - webhook)    │
                    └──────────────────────────┘
```

### Components

| Component | File | Responsibility |
|-----------|------|----------------|
| Horizon Reader | `src/readers/horizon.ts` | Polls Horizon API for classic-bridge effects related to StreamPay streams |
| Soroban RPC Reader | `src/readers/soroban.ts` | Queries Soroban RPC `getContractData` for current contract storage state |
| Reconciliation Engine | `src/engine/diff.ts` | Compares both sources, applies consistency lag window, detects divergences |
| Alerting Service | `src/alerting.ts` | Structured JSON logging + webhook dispatch to indexer dashboard |
| Canonical records | `src/audit/canonical.ts` | The one `StreamEntry`-shaped encoding both sides are merkleized through |
| Merkle tree | `src/audit/merkle.ts` | Sorted tree with inclusion and exclusion proofs, odd nodes promoted |
| State auditor | `src/audit/auditor.ts` | Merkleizes both reads, classifies each difference, emits witnesses |
| Publication | `src/audit/publication.ts` | Checkpointed, rate-bounded continuous loop writing roots + proofs |
| Public verifier | `src/audit/verifier.ts` | Recomputes everything from published artifacts alone |

---

## Environment Variables

Create a `.env` file in `reconciler/` (copy from `.env.example`):

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `HORIZON_URL` | Yes | `https://horizon-testnet.stellar.org` | Horizon API endpoint |
| `HORIZON_API_TOKEN` | No | — | Optional Horizon API token |
| `SOROBAN_RPC_URL` | Yes | `https://soroban-testnet.stellar.org/rpc` | Soroban RPC endpoint |
| `SOROBAN_RPC_TOKEN` | No | — | Optional Soroban RPC token |
| `WEBHOOK_URL` | Yes | `http://localhost:8080/webhooks/reconciler` | Indexer dashboard webhook endpoint (Issue #2) |
| `CONSISTENCY_WINDOW_MS` | No | `30000` | Maximum acceptable lag between Horizon and Soroban data (ms) |
| `POLL_INTERVAL_MS` | No | `5000` | Delay between reconciliation polls (ms) |
| `LOG_LEVEL` | No | `INFO` | `DEBUG\|INFO\|WARN\|ERROR` |
| `ENVIRONMENT` | No | `testnet` | Deployment environment tag |
| `RECONCILER_ID` | No | `primary-001` | Unique instance identifier for multi-instance deployments |
| `CONTRACT_ID` | No | — | StreamPay Soroban contract ID |
| `STREAM_IDS` | No | — | Comma-separated stream IDs to monitor |
| `AUDIT_DIR` | No | `audit-out` | Where report/bundle/artifact/checkpoint files are written |
| `AUDIT_INTERVAL_MS` | No | `60000` | Minimum spacing between audit cycles |
| `AUDIT_ARTIFACT_LIMIT` | No | `2000` | Publish the full record set as an artifact when each side has at most this many records (`0` disables it) |
| `AUDIT_TOLERANCE_EPSILON` | No | `1` | Amount tolerance grid; `1` tolerates nothing, `10` treats sub-10-unit accrual as rounding |
| `AUDIT_FLAP_WINDOW_MS` | No | `120000` | How long a key must hold one state before a transition pages anyone |

---

## Continuous merkleized state audit

Every cycle the auditor merkleizes **both** sides of the read — the contract
state and its own projection — and publishes the four roots together with the
ledger sequence they describe. A root difference is then explained, not
asserted: each divergent record gets a witness carrying its canonical tuple
plus an inclusion proof on the side that has it and an exclusion proof on the
side that does not.

| Verdict | Meaning |
|---------|---------|
| `clean` | Both raw roots are identical |
| `rounding_only` | Amounts differ but every difference lands on the same `AUDIT_TOLERANCE_EPSILON` grid point |
| `fault` | A value differs beyond tolerance, or a record exists on one side only |

Cycles are checkpointed (`AUDIT_DIR/<reconciler-id>-checkpoint.json`), so a
restart resumes the incident table instead of re-paging, and rate-bounded by
`AUDIT_INTERVAL_MS`. Alerts are de-duplicated: a persisting divergence is one
incident, and a key that flickers inside `AUDIT_FLAP_WINDOW_MS` does not page.

```bash
npm run audit            # one cycle, writes report + bundle + artifact
node dist/index.js audit # continuous audit loop
```

### Verifying a publication without trusting the auditor

The verifier reads only published files — no readers, no credentials:

```bash
node dist/audit/verifier.js --bundle  audit-out/primary-001-bundle.json
node dist/audit/verifier.js --report  audit-out/primary-001-report.json \
                              --artifact audit-out/primary-001-artifact.json
```

It re-derives every leaf hash from the witness tuples, folds every Merkle path
against the published roots, re-runs the tolerance classification from the
bundle's own policy digest, and recomputes the verdict. Exit code 0 means the
whole artifact reproduced; 1 means something published does not recompute.
`testdata/` holds a committed divergent publication that CI re-verifies on
every pull request; regenerate it with `npm run testdata` only when the
canonical encoding, the tree shape or the verdict rules change on purpose.

---

## Consistency Lag Window Behavior

The reconciliation engine applies a **consistency lag window** to tolerate temporary asynchrony between Horizon and Soroban RPC:

| Scenario | Behavior |
|----------|----------|
| Both sources agree | No alert; logged as `INFO` |
| Sources disagree, but timestamps within `CONSISTENCY_WINDOW_MS` | Treated as expected lag; logged as `WARN`, no webhook |
| Sources disagree, timestamps outside `CONSISTENCY_WINDOW_MS` | Divergence detected; logged as `ERROR`; webhook dispatched |
| Stream missing from Horizon only | Warning (likely indexing delay) |
| Stream missing from Soroban only | Critical (potential index corruption) |

Default `CONSISTENCY_WINDOW_MS=30000` (30 seconds) reflects typical Horizon indexing lag on Stellar testnet.

---

## Structured Log Format

All logs are emitted as JSON to stderr:

```json
{
  "timestamp": "2026-10-16T16:20:32.123Z",
  "service": "streampay-reconciler",
  "level": "ERROR",
  "message": "Stream 123 divergence detected",
  "details": {
    "streamId": "123",
    "checkType": "stream_balance",
    "indexedValue": {"deposit": "1000", "withdrawn": "200"},
    "onChainValue": {"deposit": "1000", "withdrawn": "500"},
    "lagMs": 45000,
    "webhookSent": true,
    "webhookResponse": {"status": 200, "message": "Alert delivered"}
  }
}
```

Fields: `timestamp`, `service`, `level`, `message`, `details`.

---

## Webhook Payload

Webhooks are dispatched to `WEBHOOK_URL` when a **critical** divergence is detected:

```json
{
  "alert": {
    "type": "stream_divergence",
    "severity": "critical",
    "timestamp": "2026-10-16T16:20:32.123Z",
    "source": "reconciler",
    "data": {
      "streamId": "123",
      "divergenceType": "stream_balance",
      "horizonState": {"deposit": "1000", "withdrawn": "200"},
      "sorobanState": {"deposit": "1000", "withdrawn": "500"},
      "lagMs": 45000,
      "consequence": "index_corruption"
    }
  },
  "metadata": {
    "version": "1.0.0",
    "environment": "testnet",
    "reconcilerId": "primary-001"
  }
}
```

This feeds into the **Issue #2 indexer dashboard withdrawal panel** for operator visibility.

---

## Setup Commands

### Local Development

```bash
# Install dependencies
cd reconciler
npm install

# Copy environment template
cp .env.example .env
# Edit .env with your Horizon URL, Soroban RPC URL, and webhook target

# Typecheck
npm run typecheck

# Run unit tests
npm test

# Run the service
npm start

# Run the soak test (injects stale RPC data to trigger divergence alarm)
npm run soak
```

### Docker Deployment

```bash
# Start reconciler and indexer
docker compose up -d

# View logs
docker compose logs -f reconciler

# Stop
docker compose down
```

---

## Testing

### Audit Tests (`src/audit/*.test.ts`)

The suite runs on the compiled output (`npm test` builds first) and covers:

| File | What it pins down |
|------|-------------------|
| `audit/canonical.test.ts` | the field set/order mirrors `StreamEntry`, tamper-sensitive bytes |
| `audit/merkle.test.ts` | inclusion + exclusion proofs, promoted odd nodes, forged-path rejection |
| `audit/tolerance.test.ts` | rounding grid vs a real divergence, non-amount fields never tolerated |
| `audit/auditor.test.ts` | divergence injection pinpoints the exact record and its proofs verify |
| `audit/dedup.test.ts` | one incident per divergence, flap suppression, restart resume |
| `audit/continuous.test.ts` | checkpointing, rate bounding, ledger-advance skip, failure budget |
| `audit/verifier.test.ts` | a publication recomputes from files alone; tampering is caught |
| `engine/diff.test.ts` | dual-read lag/divergence classification and the alert gate |

Regenerate the committed publication with `npm run testdata` when a canonical
rule changes deliberately — CI then requires `npm run verify:published` to
still reproduce it.

```bash
npm test
npm run verify:published
```

### Soak Harness (`src/tools/soak.ts`)

Deliberately injects stale Soroban RPC and Horizon responses to trigger real
divergence alarms, then runs one merkleized audit cycle and prints a PR-ready
report.

```bash
npm run soak
```

Expected scenarios:
- Baseline: both sources fresh and in sync → no divergences
- Soroban delay within window (15s) → no divergences (expected lag)
- Soroban delay outside window (45s) → divergence detected
- Horizon delay causing apparent divergence → divergence detected
- Both sources equally delayed and in agreement → no divergences

---

## Docker Compose

`reconciler/docker-compose.yml` deploys:
- **reconciler** — the dual-read reconciliation daemon (port 9092)
- **indexer** — the webhook target (port 8080, Issue #2 integration)

### Health Checks

The reconciler service includes an HTTP health check at `/health` (configured in docker-compose).

### Networks

All services communicate over the `streampay` bridge network.

---

## Integration Points

- **Horizon API**: `HORIZON_URL` — classic-bridge effects polling
- **Soroban RPC**: `SOROBAN_RPC_URL` — `getContractData` queries
- **Indexer Dashboard (Issue #2)**: `WEBHOOK_URL` — divergence alert delivery
- **Prometheus/Grafana**: via `infra/observability/` — metrics and dashboards

---

## License

Apache-2.0 — see LICENSE at repository root.