import express from "express";
import { Keypair, Networks, nativeToScVal } from "@stellar/stellar-sdk";
import { decodeEvent } from "./decoder.js";
import { generateStreamProof } from "./proof.js";
import { StreamStore } from "./store.js";
import { getGlobalRpcManager } from "./rpc-manager.js";
import type { EndpointHealth, RateBudgetSnapshot } from "./rpc-manager.js";
import { IngestPipeline, type EventsPage, type TickReport } from "./ingest/pipeline.js";
import type { SequencerMetrics } from "./ingest/sequencer.js";
import { WebhookDispatcher } from "./webhooks/dispatcher.js";
import { validateWebhookUrl } from "./webhooks/ssrf.js";
import { WebhookSubscriptionRegistry, SubscriptionError } from "./webhooks/registry.js";
import { TenantDirectory, directoryFromEnv, type Principal } from "./webhooks/tenants.js";
import { WebhookMetrics } from "./webhooks/metrics.js";
import { SecretCipher } from "./webhooks/secrets.js";
import { sanitizeAttributes, sanitizeError } from "./privacy.js";
import { OutboxDispatcher } from "./outbox/dispatcher.js";
import { OutboxMetrics } from "./outbox/metrics.js";
import { httpPushTransport } from "./outbox/http-transport.js";
import { traceCorrelationRegistry } from "./telemetry.js";
import { createGracefulShutdown, installShutdownHandlers } from "./shutdown.js";
import type { DecodedEvent, Stream } from "./types.js";

/** Express types a path parameter as `string | string[]`; routes take one id. */
function routeId(req: express.Request): string {
  return String(req.params.id ?? "");
}

function parseCursor(raw: unknown): number {
  const value = Number(raw ?? 0);
  return Number.isInteger(value) && value >= 0 ? value : -1;
}

function parseLimit(raw: unknown, def: number, max: number): number {
  const value = Number(raw ?? def);
  return Number.isInteger(value) && value >= 1 && value <= max ? value : def;
}

function envInt(name: string, def: number): number {
  const raw = process.env[name];
  if (raw === undefined) return def;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1) throw new Error(`${name} must be a positive integer`);
  return value;
}

/** Same as `envInt`, but 0 is a legitimate value. */
function envNonNegativeInt(name: string, def: number): number {
  const raw = process.env[name];
  if (raw === undefined) return def;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 0) throw new Error(`${name} must be a non-negative integer`);
  return value;
}

/** A fraction in [0, 1], the shape every SLA breach budget takes. */
function envRatio(name: string, def: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return def;
  const value = Number(raw);
  if (!Number.isFinite(value) || value < 0 || value > 1) throw new Error(`${name} must be a fraction between 0 and 1`);
  return value;
}

/** Ledger to index from on a fresh database; unset means "start at the tip". */
function configuredStartLedger(): number | undefined {
  const raw = process.env.START_LEDGER;
  if (raw === undefined || raw === "") return undefined;
  if (!/^\d+$/.test(raw)) throw new Error("START_LEDGER must be an integer ledger sequence");
  return Number(raw) > 0 ? Number(raw) : undefined;
}

/** What the ingestion pipelines report on `/health` and `/api/ingest/stats`. */
export interface IngestStats {
  lastTick: TickReport | null;
  sequencer: SequencerMetrics;
  budget: RateBudgetSnapshot;
}

/**
 * Signing material for `/streams/:id/proof` (issue #182). The proof endpoint is
 * only meaningful with a checkpoint signer, so it is supplied by `run()` from
 * the environment and left out of `createApp` calls from tests — an unconfigured
 * app answers 503 on that route instead of failing to boot.
 */
export interface ProofConfig {
  networkPassphrase: string;
  contractId: string;
  signer: Keypair;
}

/**
 * Tenant isolation for the webhook management API (issue #585). Supplied by
 * `run()`; left out of `createApp` calls from tests, which then keep the
 * historical single-tenant behaviour against the raw store.
 */
export interface WebhookApiConfig {
  directory: TenantDirectory;
  registry: WebhookSubscriptionRegistry;
  metrics?: WebhookMetrics;
  /** Tenant used when the deployment provisions no keys, i.e. trusts its transport. */
  defaultTenantId?: string;
}

function bearerToken(header: string | string[] | undefined): string | undefined {
  const raw = Array.isArray(header) ? header[0] : header;
  if (!raw) return undefined;
  const match = /^Bearer\s+(.+)$/i.exec(raw.trim());
  return match ? match[1] : undefined;
}

/**
 * Identify the caller of a webhook management request.
 *
 * With credentials provisioned, the tenant key is presented in
 * `x-streampay-webhook-key` (or a bearer token) and the tenant comes from the
 * directory — never from the request body, which is what makes the isolation
 * unforgeable. With none provisioned the deployment is declaring itself
 * single-tenant, so every request is that one tenant.
 */
function webhookPrincipal(config: WebhookApiConfig, req: express.Request): Principal | null {
  if (!config.directory.enforcesAuthentication) {
    return config.directory.principalFor(config.defaultTenantId ?? "default");
  }
  const header = req.headers["x-streampay-webhook-key"];
  const presented = typeof header === "string" && header !== "" ? header : bearerToken(req.headers.authorization);
  return config.directory.resolveKey(presented);
}

export function createApp(
  store: StreamStore,
  dispatcher?: WebhookDispatcher,
  outboxDispatcher?: OutboxDispatcher,
  outboxMetrics?: OutboxMetrics,
  rpcHealth?: () => EndpointHealth[],
  ingestStats?: () => IngestStats,
  proofConfig?: ProofConfig,
  webhookApi?: WebhookApiConfig
): express.Express {
  const app = express();
  app.use(express.json());

  app.get("/health", (_req, res) => {
    res.json({
      ok: true,
      status: "ok",
      lastLedger: store.getState("lastLedger") ?? null,
      cursor: store.getState("cursor") ?? null,
      ingest: ingestStats?.() ?? null,
      rpcEndpoints: rpcHealth?.() ?? [],
      checkpointSigner: proofConfig?.signer.publicKey() ?? null,
    });
  });

  // GET /events/stream (SSE push endpoint)
  app.get("/events/stream", (req, res) => {
    const address = req.query.address as string | undefined;
    const streamId = req.query.streamId as string | undefined;
    const lastEventId = (req.headers["last-event-id"] as string | undefined) ?? (req.query.lastEventId as string | undefined);

    if (address && !/^[GCB][A-Z2-7]{55}$/.test(address)) {
      return res.status(400).json({ error: "invalid Stellar address" });
    }
    if (streamId && !/^\d+$/.test(streamId)) {
      return res.status(400).json({ error: "stream id must be an unsigned integer" });
    }

    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      "Connection": "keep-alive",
      "X-Accel-Buffering": "no",
      "Access-Control-Allow-Origin": "*",
    });

    if (typeof (res as unknown as { flushHeaders?: () => void }).flushHeaders === "function") {
      (res as unknown as { flushHeaders: () => void }).flushHeaders();
    }

    // Initial keepalive greeting
    res.write(": ok\n\n");

    const formatSse = (evt: {
      id: string;
      /** Null for an event the registry decoded without a stream id binding. */
      streamId: string | null;
      name: string;
      ledger: number;
      ledgerClosedAt: string;
      payload: Record<string, unknown>;
      stream?: unknown;
    }): string => {
      const data = JSON.stringify({
        id: evt.id,
        streamId: evt.streamId,
        eventType: evt.name,
        ledger: evt.ledger,
        ledgerClosedAt: evt.ledgerClosedAt,
        data: evt.payload,
        stream: evt.stream,
      });
      return `id: ${evt.id}\nevent: ${evt.name}\ndata: ${data}\n\n`;
    };

    // Replay historical events if lastEventId was supplied
    if (lastEventId) {
      const historical = store.getEventsAfter(lastEventId, address, streamId);
      for (const evt of historical) {
        res.write(formatSse(evt));
      }
    }

    // Subscribe to live events
    const onLiveEvent = (event: DecodedEvent, stream?: Stream) => {
      if (streamId && event.streamId !== streamId) {
        return;
      }
      if (address && stream && stream.sender !== address && stream.recipient !== address) {
        return;
      }
      res.write(
        formatSse({
          id: event.id,
          streamId: event.streamId,
          name: event.name,
          ledger: event.ledger,
          ledgerClosedAt: event.ledgerClosedAt,
          payload: event.data as Record<string, unknown>,
          stream,
        })
      );
    };

    store.on("event", onLiveEvent);

    // Heartbeat ping every 15 seconds to prevent reverse proxy/load-balancer timeouts
    const pingInterval = setInterval(() => {
      res.write(": ping\n\n");
    }, 15000);

    req.on("close", () => {
      clearInterval(pingInterval);
      store.off("event", onLiveEvent);
      res.end();
    });
  });

  // Trace correlation endpoints for distributed tracing across the async event seam
  app.post("/api/traces/correlate", (req, res) => {
    const { txHash, traceContext, streamId } = req.body || {};
    if (!txHash || !traceContext?.traceId || !traceContext?.spanId) {
      return res.status(400).json({ error: "txHash, traceContext.traceId, and traceContext.spanId are required" });
    }
    traceCorrelationRegistry.register(String(txHash), traceContext, streamId ? String(streamId) : undefined);
    return res.status(200).json({ ok: true, txHash, streamId });
  });

  app.get("/api/traces/correlate", (req, res) => {
    const txHash = req.query.txHash as string | undefined;
    const streamId = req.query.streamId as string | undefined;
    const context = (txHash ? traceCorrelationRegistry.get(txHash) : null) || (streamId ? traceCorrelationRegistry.getByStream(streamId) : null);
    if (!context) {
      return res.status(404).json({ error: "correlation not found" });
    }
    return res.json({ ok: true, context });
  });

  // Full ingestion accounting for the parallel ingester (issue #117): the depth
  // the budget allowed, how deep the reorder buffer actually got, and the
  // late/duplicate/gap counters that tell an operator whether the ordering
  // guards are being exercised in production.
  app.get("/api/ingest/stats", (_req, res) => {
    const stats = ingestStats?.();
    return stats ? res.json(stats) : res.status(503).json({ error: "ingestion not running" });
  });

  // GET /streams?recipient=<address>
  app.get("/streams", (req, res) => {
    const recipient = req.query.recipient as string | undefined;
    if (!recipient) {
      return res.json([]);
    }
    const streams = store.listStreamsByRecipient(recipient);
    return res.json(streams);
  });

  // GET /streams/:id/history
  app.get("/streams/:id/history", (req, res) => {
    const streamId = routeId(req);
    if (!streamId) {
      return res.json([]);
    }
    const history = store.getStreamHistory(streamId);
    return res.json(history);
  });

  // GET /streams/:id
  app.get("/streams/:id", (req, res) => {
    if (!/^\d+$/.test(routeId(req))) return res.status(400).json({ error: "stream id must be an unsigned integer" });
    const stream = store.getStream(routeId(req));
    return stream ? res.json(stream) : res.status(404).json({ error: "stream not found" });
  });

  // GET /streams/:id/proof — light-client checkpoint proof (#182). The indexer
  // hashes every stream it has projected into a Merkle state root and signs the
  // checkpoint, so a client can verify one stream's state against the signed root
  // instead of trusting this endpoint's word for it. `streamToProofEntry` builds
  // the canonical ledger-entry XDR, so the leaf the client re-hashes is the leaf
  // the chain would have produced.
  app.get("/streams/:id/proof", (req, res) => {
    if (!/^\d+$/.test(routeId(req))) return res.status(400).json({ error: "stream id must be an unsigned integer" });
    if (!proofConfig) return res.status(503).json({ error: "proof endpoint is not configured: set CHECKPOINT_SIGNER_SECRET" });
    const stream = store.getStream(routeId(req));
    if (!stream) return res.status(404).json({ error: "stream not found" });
    const ledger = Number(store.getState("lastLedger") ?? stream.lastLedger);
    const ledgerCloseTime = Math.floor(new Date(stream.updatedAt).getTime() / 1000) || Math.floor(Date.now() / 1000);
    const proof = generateStreamProof({
      networkPassphrase: proofConfig.networkPassphrase,
      contractId: proofConfig.contractId,
      ledger,
      ledgerCloseTime,
      streams: store.listAllStreams(),
      targetStreamId: routeId(req),
      signer: proofConfig.signer,
    });
    return res.json(proof);
  });

  // GET /streams/:id/events?after=<seq>&limit=<n> — transactional-outbox replay.
  // Consumers replay from their last durable cursor; duplicates (at-least-once
  // transport) are detectable via the per-row idempotency_key and seq ordering.
  app.get("/streams/:id/events", (req, res) => {
    if (!/^\d+$/.test(routeId(req))) return res.status(400).json({ error: "stream id must be an unsigned integer" });
    const after = parseCursor(req.query.after);
    if (after < 0) return res.status(400).json({ error: "after must be a non-negative integer cursor" });
    const limit = parseLimit(req.query.limit, 100, 1000);
    return res.json(store.outbox.replayEvents(routeId(req), after, limit));
  });

  // ---------- outbox management API ----------

  app.get("/api/outbox/consumers", (_req, res) => res.json(store.outbox.listConsumers()));

  app.post("/api/outbox/consumers", async (req, res) => {
    const { id, kind, url, secret, max_attempts: maxAttempts, rate_limit_per_sec: rateLimit } = req.body || {};
    if (kind !== "push" && kind !== "pull") return res.status(400).json({ error: "kind must be push or pull" });
    if (kind === "push") {
      if (!url || typeof url !== "string") return res.status(400).json({ error: "push consumers require a url" });
      const ssrf = await validateWebhookUrl(url);
      if (!ssrf.safe) return res.status(400).json({ error: `SSRF validation failed: ${ssrf.reason}` });
    }
    try {
      const consumer = store.outbox.createConsumer({
        id: typeof id === "string" ? id : undefined,
        kind,
        url: typeof url === "string" ? url : null,
        secret: typeof secret === "string" ? secret : null,
        max_attempts: typeof maxAttempts === "number" ? maxAttempts : undefined,
        rate_limit_per_sec: typeof rateLimit === "number" && rateLimit > 0 ? rateLimit : null,
      });
      return res.status(201).json(consumer);
    } catch (err: unknown) {
      return res.status(400).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  app.get("/api/outbox/consumers/:id", (req, res) => {
    const consumer = store.outbox.getConsumer(routeId(req));
    return consumer ? res.json(consumer) : res.status(404).json({ error: "consumer not found" });
  });

  app.post("/api/outbox/consumers/:id/status", (req, res) => {
    const status = req.body?.status;
    if (status !== "active" && status !== "disabled") return res.status(400).json({ error: "status must be active or disabled" });
    const ok = store.outbox.setConsumerStatus(routeId(req), status);
    return ok ? res.json({ ok: true }) : res.status(404).json({ error: "consumer not found" });
  });

  app.delete("/api/outbox/consumers/:id", (req, res) => {
    const deleted = store.outbox.deleteConsumer(routeId(req));
    return deleted ? res.json({ ok: true }) : res.status(404).json({ error: "consumer not found" });
  });

  app.get("/api/outbox/intents", (req, res) => {
    const state = req.query.state as string | undefined;
    if (state && !["pending", "in_flight", "delivered", "dead"].includes(state)) {
      return res.status(400).json({ error: "invalid state filter" });
    }
    const limit = parseLimit(req.query.limit, 50, 500);
    return res.json(store.outbox.listIntents({
      state: state as "pending" | undefined,
      consumerId: typeof req.query.consumer === "string" ? req.query.consumer : undefined,
      limit,
    }));
  });

  app.post("/api/outbox/intents/:id/replay", (req, res) => {
    const id = Number(routeId(req));
    if (!Number.isInteger(id) || id < 1) return res.status(400).json({ error: "intent id must be a positive integer" });
    const replayed = store.outbox.replayIntent(id);
    return replayed ? res.json({ ok: true }) : res.status(404).json({ error: "dead-letter intent not found" });
  });

  // Reorg retraction hook, called by the dual-read reconciliation service when
  // the indexer journal diverges from on-chain truth above a ledger boundary.
  app.post("/api/outbox/retract", (req, res) => {
    const afterLedger = Number(req.body?.after_ledger);
    if (!Number.isInteger(afterLedger) || afterLedger < 0) return res.status(400).json({ error: "after_ledger must be a non-negative integer" });
    const reason = typeof req.body?.reason === "string" ? req.body.reason : "reorg";
    const result = store.outbox.retractAfterLedger(afterLedger, reason);
    outboxMetrics?.observeRetract(result.events_retracted, result.intents_retracted);
    return res.json(result);
  });

  app.get("/api/outbox/stats", (_req, res) => res.json(store.outbox.stats()));

  app.post("/api/outbox/flush", async (_req, res) => {
    if (!outboxDispatcher) return res.status(503).json({ error: "outbox dispatcher not configured" });
    return res.json(await outboxDispatcher.tick());
  });

  // Prometheus text exposition on the indexer's own surface.
  app.get("/metrics", (_req, res) => {
    const sections: string[] = [];
    if (outboxMetrics) sections.push(outboxMetrics.render(store.outbox.stats()).trimEnd());
    if (webhookApi?.metrics) {
      sections.push(webhookApi.metrics.render(store.webhooks.deliveryStats()).trimEnd());
    }
    if (sections.length === 0) sections.push("# no metrics configured");
    res.setHeader("Content-Type", "text/plain; version=0.0.4");
    return res.send(`${sections.join("\n")}\n`);
  });

  // GET /address/:addr/streams
  app.get("/address/:addr/streams", (req, res) => {
    const address = req.params.addr;
    if (!/^[GCB][A-Z2-7]{55}$/.test(address)) return res.status(400).json({ error: "invalid Stellar address" });
    const streams = store.listAddress(address);
    return res.json({ address, count: streams.length, streams });
  });

  // ── Webhook management API ─────────────────────────────────────────────────
  // With a registry configured every route is tenant-scoped: the tenant comes
  // from the presented credential, another tenant's id reads as 404, and no
  // response but the one that mints a secret ever carries secret material.
  // Without one, the routes behave as they always did for a single-tenant
  // deployment.
  const tenantRoute =
    (handler: (principal: Principal, req: express.Request, res: express.Response) => unknown | Promise<unknown>) =>
    async (req: express.Request, res: express.Response): Promise<void> => {
      let principal: Principal = { tenantId: "", keyId: "legacy", authenticated: false };
      if (webhookApi) {
        try {
          principal = webhookPrincipal(webhookApi, req) ?? principal;
          if (principal.keyId === "legacy") {
            res.status(401).json({
              error: "a tenant webhook key is required in x-streampay-webhook-key",
              code: "unauthenticated",
            });
            return;
          }
        } catch (err: unknown) {
          res.status(503).json({ error: sanitizeError(err), code: "tenant_unprovisioned" });
          return;
        }
      }
      try {
        const result = await handler(principal, req, res);
        if (result !== undefined && !res.headersSent) res.json(result);
      } catch (err: unknown) {
        if (err instanceof SubscriptionError) {
          res.status(err.status).json(err.toJSON());
          return;
        }
        res.status(500).json({ error: sanitizeError(err), code: "internal_error" });
      }
    };

  /** Registry-only routes answer 503 for a deployment that runs the legacy single-tenant store. */
  const registryOnly = (res: express.Response): boolean => {
    if (webhookApi) return true;
    res.status(503).json({ error: "webhook registry not configured", code: "registry_unavailable" });
    return false;
  };

  app.get(
    "/api/webhooks/endpoints",
    tenantRoute((principal) => {
      if (!webhookApi) return store.webhooks.listEndpoints();
      return webhookApi.registry.list(principal).subscriptions;
    })
  );

  app.post(
    "/api/webhooks/endpoints",
    tenantRoute(async (principal, req, res) => {
      if (!webhookApi) {
        const { url, events, secret } = req.body || {};
        if (!url || typeof url !== "string") {
          res.status(400).json({ error: "URL is required" });
          return;
        }
        const ssrf = await validateWebhookUrl(url);
        if (!ssrf.safe) {
          res.status(400).json({ error: `SSRF validation failed: ${ssrf.reason}` });
          return;
        }
        return store.webhooks.createEndpoint({
          url,
          events: Array.isArray(events) ? events : ["*"],
          secret: typeof secret === "string" ? secret : undefined,
        });
      }
      const { url, events, secret } = req.body || {};
      const created = await webhookApi.registry.create(principal, {
        url,
        events: Array.isArray(events) ? events : undefined,
        secret: typeof secret === "string" ? secret : undefined,
      });
      // The signing secret is disclosed here and never again.
      res.status(201);
      return { ...created.subscription, secret: created.secret_once };
    })
  );

  app.get(
    "/api/webhooks/endpoints/:id",
    tenantRoute((principal, req, res) => {
      if (!webhookApi) {
        const ep = store.webhooks.getEndpoint(routeId(req));
        if (!ep) {
          res.status(404).json({ error: "Endpoint not found" });
          return;
        }
        return ep;
      }
      return webhookApi.registry.get(principal, routeId(req));
    })
  );

  app.patch(
    "/api/webhooks/endpoints/:id",
    tenantRoute(async (principal, req, res) => {
      if (!registryOnly(res)) return;
      const { url, events, status } = req.body || {};
      return webhookApi!.registry.update(principal, routeId(req), {
        url,
        events: Array.isArray(events) ? events : undefined,
        status: status === "active" || status === "disabled" ? status : undefined,
      });
    })
  );

  app.post(
    "/api/webhooks/endpoints/:id/rotate-secret",
    tenantRoute((principal, req, res) => {
      if (!webhookApi) {
        try {
          return store.webhooks.rotateEndpointSecret(routeId(req));
        } catch (err: unknown) {
          res.status(404).json({ error: sanitizeError(err) });
          return;
        }
      }
      const rotated = webhookApi.registry.rotateSecret(principal, routeId(req));
      return {
        secret: rotated.secret_once,
        previous_secret: null,
        fingerprint: rotated.fingerprint,
        previous_fingerprint: rotated.previous_fingerprint,
        expires_at: rotated.expires_at,
      };
    })
  );

  app.get(
    "/api/webhooks/endpoints/:id/rotations",
    tenantRoute((principal, req, res) => {
      if (!registryOnly(res)) return;
      return webhookApi!.registry.rotationHistory(principal, routeId(req), parseLimit(req.query.limit, 20, 100));
    })
  );

  app.get(
    "/api/webhooks/quota",
    tenantRoute((principal, _req, res) => {
      if (!registryOnly(res)) return;
      return webhookApi!.registry.list(principal).quota;
    })
  );

  app.delete(
    "/api/webhooks/endpoints/:id",
    tenantRoute((principal, req, res) => {
      if (!webhookApi) {
        const deleted = store.webhooks.deleteEndpoint(routeId(req));
        if (!deleted) {
          res.status(404).json({ error: "Endpoint not found" });
          return;
        }
        return { ok: true };
      }
      return webhookApi.registry.remove(principal, routeId(req));
    })
  );

  app.post(
    "/api/webhooks/endpoints/:id/test",
    tenantRoute(async (principal, req, res) => {
      if (!dispatcher) {
        res.status(503).json({ error: "Webhook dispatcher not configured", code: "dispatcher_unavailable" });
        return;
      }
      // Ownership is checked before the dispatcher, which is tenant-blind.
      if (webhookApi) webhookApi.registry.get(principal, routeId(req));
      return await dispatcher.testFireEndpoint(routeId(req));
    })
  );

  app.get(
    "/api/webhooks/deliveries",
    tenantRoute((principal, req) => {
      const limit = Math.min(100, Math.max(1, Number(req.query.limit || 50)));
      if (!webhookApi) return store.webhooks.listDeliveries(limit);
      return webhookApi.registry.deliveries(principal, limit);
    })
  );

  app.get(
    "/api/webhooks/dlq",
    tenantRoute((principal, req) => {
      const limit = Math.min(100, Math.max(1, Number(req.query.limit || 50)));
      if (!webhookApi) return store.webhooks.listDLQ(limit);
      return webhookApi.registry.dlq(principal, limit);
    })
  );

  app.post(
    "/api/webhooks/dlq/:id/retry",
    tenantRoute((principal, req, res) => {
      if (!webhookApi) {
        const success = store.webhooks.replayDLQItem(routeId(req));
        if (!success) {
          res.status(404).json({ error: "DLQ item not found" });
          return;
        }
        return { ok: true, replayed: true };
      }
      return webhookApi.registry.replayDlq(principal, routeId(req));
    })
  );

  app.get(
    "/api/webhooks/sla",
    tenantRoute((principal, _req, res) => {
      if (!registryOnly(res)) return;
      return webhookApi!.registry.sla(principal);
    })
  );

  app.post(
    "/api/webhooks/rotations/expire",
    tenantRoute((_principal, _req, res) => {
      if (!registryOnly(res)) return;
      return webhookApi!.registry.retireExpiredRotations();
    })
  );

  return app;
}

export async function run(): Promise<void> {
  const rpcUrl = process.env.RPC_URL ?? "https://soroban-testnet.stellar.org";
  const contractId = process.env.CONTRACT_ID;
  const pollInterval = Number(process.env.POLL_INTERVAL_MS ?? 5000);
  const port = Number(process.env.PORT ?? 8787);
  const dbPath = process.env.DATABASE_PATH ?? "./data/streampay.sqlite";

  if (!contractId) throw new Error("CONTRACT_ID is required");
  if (!Number.isFinite(pollInterval) || pollInterval < 250) throw new Error("POLL_INTERVAL_MS must be at least 250");

  const webhookCipher = SecretCipher.fromEnv();
  if (webhookCipher === null) {
    console.warn(
      "[indexer] WEBHOOK_SECRET_ENCRYPTION_KEY is unset: webhook signing secrets are stored in the clear. " +
        "Set it to a 32-byte hex key before serving more than one tenant."
    );
  }
  const store = new StreamStore(dbPath, { webhookCipher });

  const { directory, defaultTenantId } = directoryFromEnv();
  const webhookMetrics = new WebhookMetrics({
    slaMs: envInt("WEBHOOK_SLA_MS", 5_000),
    breachBudget: envRatio("WEBHOOK_SLA_BREACH_BUDGET", 0.01),
    minSamples: envInt("WEBHOOK_SLA_MIN_SAMPLES", 20),
  });
  const webhookRegistry = new WebhookSubscriptionRegistry({
    store: store.webhooks,
    directory,
    metrics: webhookMetrics,
    rotationOverlapMs: envInt("WEBHOOK_SECRET_ROTATION_OVERLAP_MS", 24 * 60 * 60 * 1000),
    requireEncryption: process.env.WEBHOOK_REQUIRE_ENCRYPTION === "1",
    // One JSON line per registry action, with the URL masked and the secret
    // represented only by its fingerprint.
    onAudit: (event) =>
      console.log(
        JSON.stringify({
          service: "streampay-indexer",
          component: "webhooks",
          ...sanitizeAttributes({ ...event, fingerprint: undefined, secret_fingerprint: event.fingerprint }),
        })
      ),
  });

  // ── Checkpoint signer for `/streams/:id/proof` (#182) ──────────────────────
  // An ephemeral `Keypair.random()` keeps a throwaway indexer running, but its
  // signatures die with the process and a client cannot pin them, so a real
  // deploy must set CHECKPOINT_SIGNER_SECRET.
  const checkpointSigner = process.env.CHECKPOINT_SIGNER_SECRET
    ? Keypair.fromSecret(process.env.CHECKPOINT_SIGNER_SECRET)
    : Keypair.random();
  const networkPassphrase = process.env.NETWORK_PASSPHRASE ?? Networks.TESTNET;

  // ── RPC manager: shared across the indexer, replaces the ad-hoc fetch helper.
  // Additional endpoints can be added via RPC_URL_FALLBACK_1, RPC_URL_FALLBACK_2 env vars.
  const rpcEndpoints = [{ url: rpcUrl }];
  for (let i = 1; i <= 5; i++) {
    const extra = process.env[`RPC_URL_FALLBACK_${i}`];
    if (extra) rpcEndpoints.push({ url: extra });
  }
  const manager = getGlobalRpcManager({
    endpoints: rpcEndpoints,
    maxRps: Number(process.env.RPC_MAX_RPS ?? "10"),
    failureThreshold: 3,
    lagThresholdSeconds: 60,
    hedgeDelayMs: 300,
    healthCheckIntervalMs: 15_000,
    timeoutMs: 20_000,
  });

  const dispatcher = new WebhookDispatcher(store.webhooks, 2000, { metrics: webhookMetrics });
  dispatcher.start();

  const outboxMetrics = new OutboxMetrics({
    alarmWindow: envInt("OUTBOX_ALARM_WINDOW", 8),
    alarmMinDepth: envInt("OUTBOX_ALARM_MIN_DEPTH", 1000),
  });
  const outboxDispatcher = new OutboxDispatcher(store.outbox, httpPushTransport, outboxMetrics, {
    tickMs: envInt("OUTBOX_TICK_MS", 1000),
    batchSize: Math.min(1000, envInt("OUTBOX_BATCH_SIZE", 200)),
    claimTimeoutMs: envInt("OUTBOX_CLAIM_TIMEOUT_MS", 60_000),
    maxDepth: process.env.OUTBOX_MAX_DEPTH ? envInt("OUTBOX_MAX_DEPTH", 1) : undefined,
  });
  outboxDispatcher.start();

  // `stream_refund_target_changed` (#204) is projected into `streams.refund_target`
  // by the store, so it has to be fetched for that column to ever be filled.
  // `batch_withdraw_summary` (#205) names no single stream — it is keyed by the
  // recipient and covers the whole `withdraw_many` call. The per-stream
  // `withdrawn` events the same call publishes remain the authoritative source
  // for the projections, so subscribing to the summary adds the batch record to
  // the journal without touching any row.
  const eventNames = [
    "stream_created",
    "withdrawn",
    "stream_cancelled",
    "stream_topped_up",
    "stream_paused",
    "stream_resumed",
    "stream_refund_target_changed",
    "batch_withdraw_summary",
  ];
  const topicFilters = eventNames.map((name) => [nativeToScVal(name, { type: "symbol" }).toXDR("base64")]);

  // ── Parallel ledger-window ingestion (issue #117) ──────────────────────────
  // Concurrency is not a constant here: the pipeline reads the same rate budget
  // the RPC manager enforces (issue #56/#100) and never plans more in-flight
  // requests than that budget can fund. INGEST_MAX_IN_FLIGHT only lowers the
  // ceiling, it cannot raise the budget.
  let lastTick: TickReport | undefined;
  const pipeline = new IngestPipeline({
    source: {
      getLatestLedger: () => manager.getLatestLedger(),
      getEvents: (params) => manager.getEvents(params) as Promise<EventsPage>,
      getRateBudget: () => manager.getRateBudget(),
    },
    sink: store,
    decode: (event) => decodeEvent(event, { contractId }),
    filters: [{ type: "contract", contractIds: [contractId], topics: topicFilters }],
    initialLedger: configuredStartLedger(),
    pageLimit: envInt("INGEST_PAGE_LIMIT", 1000),
    windowLedgers: envInt("INGEST_WINDOW_LEDGERS", 20),
    maxWindowLedgers: envInt("INGEST_MAX_WINDOW_LEDGERS", 5_000),
    maxWindowsPerTick: envInt("INGEST_MAX_WINDOWS_PER_TICK", 200),
    maxInFlight: envInt("INGEST_MAX_IN_FLIGHT", 8),
    confirmations: envNonNegativeInt("INGEST_CONFIRMATIONS", 0),
    maxPagesPerWindow: envInt("INGEST_MAX_PAGES_PER_WINDOW", 8),
    parallel: process.env.INGEST_PARALLEL !== "0",
    sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  });

  const app = createApp(
    store,
    dispatcher,
    outboxDispatcher,
    outboxMetrics,
    () => manager.getHealth(),
    () => ({
      lastTick: lastTick ?? null,
      sequencer: pipeline.metrics(),
      budget: manager.getRateBudget(),
    }),
    { networkPassphrase, contractId, signer: checkpointSigner },
    { directory, registry: webhookRegistry, metrics: webhookMetrics, defaultTenantId }
  );

  async function poll(): Promise<void> {
    lastTick = await pipeline.runTick();
    if (!lastTick.ok) throw new Error(`ingest tick failed: ${lastTick.error ?? "unknown error"}`);
    if (!lastTick.caughtUp) {
      console.warn(`[indexer] tick stopped at ledger ${lastTick.resumeAt ?? "?"} with a lagging endpoint or dense window; resuming next tick`);
    }
  }

  const server = app.listen(port, () => console.log(`StreamPay indexer listening on http://localhost:${port}`));
  let inFlightPoll: Promise<void> | null = null;
  let shuttingDown = false;

  async function pollSafely(): Promise<void> {
    if (inFlightPoll || shuttingDown) return;
    inFlightPoll = (async () => {
      try {
        await poll();
      } catch (error) {
        console.error("Indexer poll failed:", error);
      }
    })();
    try {
      await inFlightPoll;
    } finally {
      inFlightPoll = null;
    }
  }

  void pollSafely();
  const pollTimer = setInterval(() => void pollSafely(), pollInterval);

  // SIGTERM/SIGINT: drain in-flight ingest and outbox dispatch, bounded by
  // SHUTDOWN_DRAIN_MS, before closing the store (see ./shutdown.ts).
  const shutdown = createGracefulShutdown({
    drainMs: envInt("SHUTDOWN_DRAIN_MS", 10_000),
    stopAccepting: () => {
      shuttingDown = true;
      clearInterval(pollTimer);
      server.close();
      // SSE streams never end on their own; without this `close()` would wait forever.
      server.closeAllConnections();
    },
    pipeline,
    inFlightIngest: () => inFlightPoll,
    outbox: outboxDispatcher,
    webhooks: dispatcher,
    store,
    exit: (code) => process.exit(code),
    log: (message) => console.warn(message),
  });
  installShutdownHandlers(process, ["SIGINT", "SIGTERM"], shutdown);
}

// Auto-run if executed directly as entrypoint
if (process.argv[1] && (process.argv[1].endsWith("index.js") || process.argv[1].endsWith("index.ts"))) {
  void run();
}
