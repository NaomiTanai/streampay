# StreamPay Event Indexer

The StreamPay Event Indexer is a standalone TypeScript Node.js service that polls Soroban RPC `getEvents` for StreamPay smart contract events, decodes the contract's typed XDR event payloads using `@stellar/stellar-sdk`, persists stream metadata and an append-only event log in SQLite, maintains cursor persistence, and serves a lightweight HTTP query API.

The service indexes the contract's emitted event topics: `stream_created`, `withdrawn`, `stream_cancelled`, and `stream_topped_up` (the Rust event types are `StreamCreated`, `Withdrawn`, `StreamCancelled`, and `StreamToppedUp`). It also recognizes `stream_paused` and `stream_resumed` for forward-compatible status updates. Unknown topics are filtered out at RPC.

---

## 1. Prerequisites

- **Node.js**: `v22.0.0` or higher
- **npm**: `v10+` or `v11+`

### Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `RPC_URL` | `https://soroban-testnet.stellar.org` | Stellar Soroban RPC URL |
| `CONTRACT_ID` | Required | StreamPay contract address (`C...`) |
| `POLL_INTERVAL_MS` | `5000` | Delay between polls (minimum 250 ms) |
| `PORT` | `8787` | HTTP listen port |
| `DATABASE_PATH` | `./data/streampay.sqlite` | SQLite database file |
| `START_LEDGER` | Latest RPC ledger | Initial ledger sequence if starting fresh without a saved cursor |
| `OUTBOX_TICK_MS` | `1000` | Transactional-outbox dispatcher poll interval |
| `OUTBOX_BATCH_SIZE` | `200` | Max intent claims per consumer per tick (cap 1000) |
| `OUTBOX_CLAIM_TIMEOUT_MS` | `60000` | Stale-claim reclaim threshold (crash recovery) |
| `OUTBOX_MAX_DEPTH` | off | Global backpressure threshold for pending+in-flight intents |
| `WEBHOOK_SECRET_ENCRYPTION_KEY` | off | 64-hex (or passphrase) AES-256-GCM key that seals signing secrets at rest |
| `WEBHOOK_REQUIRE_ENCRYPTION` | off | `1` refuses plaintext secret storage; creations answer `503` without a key |
| `WEBHOOK_TENANT_KEYS` | off | `tenantId:keyId:secret` triples, comma-separated (only digests are kept) |
| `WEBHOOK_DEFAULT_TENANT_ID` | `default` | Tenant used when no directory is configured |
| `WEBHOOK_TENANT_QUOTA_SUBSCRIPTIONS` | `20` | Subscriptions a tenant may hold |
| `WEBHOOK_TENANT_QUOTA_EVENTS` | `24` | Event names one subscription may select |
| `WEBHOOK_TENANT_QUOTA_ROTATIONS` | `6` | Secret rotations per rolling hour, per subscription |
| `WEBHOOK_SECRET_ROTATION_OVERLAP_MS` | `86400000` | How long a rotated-out secret keeps verifying |
| `WEBHOOK_SLA_MS` | `5000` | Per-attempt delivery commitment |
| `WEBHOOK_SLA_BREACH_BUDGET` | `0.01` | Fraction of the window allowed to miss the SLA |
| `WEBHOOK_SLA_MIN_SAMPLES` | `20` | Window depth required before the breach alarm engages |
| `SHUTDOWN_DRAIN_MS` | `10000` | On SIGTERM/SIGINT, how long in-flight ingest and outbox dispatch may finish before the process exits (exit code `0` if drained, `1` if the window expired). Keep it below your orchestrator's kill grace period |

---

## 2. How to Run Locally

### Installation & Build

```bash
cd indexer
npm install
npm run typecheck
npm test
npm run build
npm start
```

### Running with Environment Variables

The service reads `.env` when your shell/environment loader provides it; Node does not load `.env` automatically. For example, on PowerShell:

```powershell
$env:RPC_URL = "https://soroban-testnet.stellar.org"
$env:CONTRACT_ID = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMDR4"
$env:PORT = "8787"
npm start
```

Or on Linux / macOS:

```bash
export RPC_URL="https://soroban-testnet.stellar.org"
export CONTRACT_ID="<YOUR_TESTNET_CONTRACT_ADDRESS>"
npm start
```

---

## 3. Database Persistence & Restarts

The SQLite database stores:
- **`streams` table**: Latest materialized state per stream (`stream_id`, `sender`, `recipient`, `deposit`, `withdrawn`, `status`, cancel amounts).
- **`events` table**: Append-only log of every received event (`id`, `stream_id`, `name`, `ledger`, `ledger_closed_at`, `payload`).
- **`indexer_state` table**: Persisted `cursor` and `lastLedger`.

Repeated events are safe and idempotent: event IDs are unique, preventing double-counting across service restarts.

---

## 4. HTTP API Reference

### Health Check
`GET /health`

```json
{
  "ok": true,
  "status": "ok",
  "lastLedger": "123456",
  "cursor": "0000000123456"
}
```

### Real-Time Event Stream (Server-Sent Events)
`GET /events/stream[?address=<address>][&streamId=<id>][&lastEventId=<id>]`

Streams live contract events as Server-Sent Events (`text/event-stream`). Supports address scoping, specific stream filtering, and resumption via `Last-Event-ID` header or query parameter. Emits heartbeat `: ping\n\n` frames every 15s.

```http
id: 0000000123456-00001
event: stream_created
data: {"id":"0000000123456-00001","streamId":"1","eventType":"stream_created","ledger":123456,"data":{...},"stream":{...}}
```

### Recipient Stream Query (Frontend discovery)
`GET /streams?recipient=<address>`

Returns all streams for the given recipient address. Returns `[]` if none found.

```json
[
  {
    "streamId": "1",
    "id": "1",
    "sender": "G...",
    "recipient": "G...",
    "token": "C...",
    "deposit": "1000000",
    "withdrawn": "250000",
    "status": "active"
  }
]
```

### Stream Event History
`GET /streams/:id/history`

Returns the full event log for a stream ID in ledger sequence order. Returns `[]` if not found.

```json
[
  {
    "id": "event-1",
    "stream_id": "1",
    "event_type": "stream_created",
    "ledger": 100,
    "payload": { ... }
  },
  {
    "id": "event-2",
    "stream_id": "1",
    "event_type": "withdrawn",
    "ledger": 105,
    "payload": { ... }
  }
]
```

### Stream Details by ID
`GET /streams/:id`

Returns the latest indexed projection, or `404` if not found:

```json
{
  "streamId": "7",
  "sender": "G...",
  "recipient": "G...",
  "token": "C...",
  "deposit": "1000000",
  "withdrawn": "250000",
  "status": "active",
  "lastLedger": 123456,
  "updatedAt": "2026-01-01T00:00:00Z"
}
```

### Streams by Address
`GET /address/:addr/streams`

Returns streams where the address is sender or recipient:

```json
{
  "address": "G...",
  "count": 1,
  "streams": [...]
}
```

---

### Transactional Outbox

Every ingested event is journalled with a monotonic `seq` and per-consumer
delivery intents inside the same database transaction, giving consumers a
replayable, dedupe-able, at-least-once event feed. Full contract and
guarantees: [docs/OUTBOX.md](../docs/OUTBOX.md).

- `GET /streams/:id/events?after=<seq>&limit=<n>` — cursor-based replay (`next_cursor` included)
- `GET/POST /api/outbox/consumers` — push/pull channel registry
- `GET /api/outbox/intents?state=dead` — outbox dead-letter queue
- `POST /api/outbox/intents/:id/replay` — resurrect a dead intent
- `POST /api/outbox/retract` — reorg retraction hook (reconciler)
- `GET /api/outbox/stats` — depth by state and replay high-water
- `GET /metrics` — Prometheus exposition (`streampay_outbox_*`)

---

### Webhook Subscriptions (shared, tenant-isolated)

One indexer serves many integrators. Subscriptions are managed through the
registry in `src/webhooks/`, which adds an identity, a quota and a measured
delivery commitment to the existing dispatcher, HMAC, SSRF guard, retry
schedule and DLQ. Full contract:
[docs/WEBHOOKS.md §7](../docs/WEBHOOKS.md).

- `GET/POST /api/webhooks/endpoints` — list the caller's subscriptions; create
  one. The `201` body is the only place the signing secret is ever disclosed.
- `GET/PATCH/DELETE /api/webhooks/endpoints/:id` — read, retarget, disable or
  remove. Reads carry `secret_fingerprint`, never the secret.
- `POST /api/webhooks/endpoints/:id/rotate-secret` — new secret, outgoing key
  keeps verifying for the overlap window (dual signatures).
- `GET /api/webhooks/endpoints/:id/rotations` — rotation history by fingerprint.
- `GET /api/webhooks/quota` · `GET /api/webhooks/sla` — the caller's budget and
  measured delivery SLA.
- `GET /api/webhooks/deliveries` · `GET /api/webhooks/dlq` ·
  `POST /api/webhooks/dlq/:id/retry` — scoped to the caller's subscriptions.
- `POST /api/webhooks/rotations/expire` — retire elapsed overlap windows.

Credentials go in `x-streampay-webhook-key` (or `Authorization: Bearer …`).
Another tenant's id answers `404`, never `403`, so the API cannot be used to
enumerate who else subscribes. With no `WEBHOOK_TENANT_KEYS` configured the
routes keep their historical single-tenant behaviour and the registry-only
routes answer `503`.

---

### Event Shapes Come From the Schema Registry

The names and fields this indexer decodes are not hand-maintained here: they
are generated from the contract's `#[contractevent]` structs into
`src/generated/events.ts` by `npm run generated:events` (source of truth:
[`schemas/events/`](../schemas/events/README.md), issue
[#213](https://github.com/Neziahtech/streampay/issues/213)).

- `decodeEvent` validates every registry-known event: the positional topic
  count must match the registry and each declared data field must be present,
  so a contract/registry drift surfaces as a decode error naming the schema
  file instead of silently indexing a half-shaped stream row.
- Extra data keys are tolerated — the registry evolves additively, and old
  indexers keep working against a newly upgraded contract.
- `src/registry.test.ts` asserts the generated table matches the committed
  `schemas/events/registry.json` field-for-field.

---

## 5. Testing

Run all unit, decoder, database, and integration tests:

```bash
npm test
```
