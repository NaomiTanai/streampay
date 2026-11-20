//! Service entrypoint for the dual-read reconciliation daemon.
//!
//! `node dist/index.js`            continuous reconciliation + merkleized audit
//! `node dist/index.js audit --once`  one audit cycle, artifacts written, then exit

import { ReconciliationEngine } from "./engine/diff.js";
import { HorizonReader } from "./readers/horizon.js";
import { SorobanRpcReader } from "./readers/soroban.js";
import { AlertingService } from "./alerting.js";
import type { StreamState } from "./types.js";
import { DEFAULT_POLICY, type AuditPolicy } from "./audit/canonical.js";
import { FileAuditSink, ContinuousAuditor, type AuditSink } from "./audit/publication.js";
import { projectionReader, type ProjectableStream } from "./audit/projection.js";
import type { StateReader } from "./audit/publication.js";
import { buildProofBundle, verifyBundle } from "./audit/proofs.js";
import { verify, type VerifyOptions } from "./audit/verifier.js";

const SERVICE_NAME = "streampay-reconciler";

export interface RuntimeConfig {
  horizonUrl: string;
  sorobanRpcUrl: string;
  webhookUrl?: string;
  consistencyWindowMs: number;
  pollIntervalMs: number;
  logLevel: "DEBUG" | "INFO" | "WARN" | "ERROR";
  contractId?: string;
  environment: string;
  reconcilerId: string;
  audit: {
    directory: string;
    intervalMs: number;
    artifactRecordLimit: number;
    toleranceEpsilon: string;
    flapWindowMs: number;
  };
}

function integerFromEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") return fallback;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer, got '${raw}'`);
  }
  return parsed;
}

export function loadConfig(): RuntimeConfig {
  const logLevel = (process.env.LOG_LEVEL || "INFO") as RuntimeConfig["logLevel"];
  if (!["DEBUG", "INFO", "WARN", "ERROR"].includes(logLevel)) {
    throw new Error(`LOG_LEVEL must be DEBUG|INFO|WARN|ERROR, got '${logLevel}'`);
  }
  return {
    horizonUrl: process.env.HORIZON_URL || "https://horizon-testnet.stellar.org",
    sorobanRpcUrl: process.env.SOROBAN_RPC_URL || "https://soroban-testnet.stellar.org/rpc",
    webhookUrl: process.env.WEBHOOK_URL || undefined,
    consistencyWindowMs: integerFromEnv("CONSISTENCY_WINDOW_MS", 30_000),
    pollIntervalMs: integerFromEnv("POLL_INTERVAL_MS", 5_000),
    logLevel,
    contractId: process.env.CONTRACT_ID,
    environment: process.env.ENVIRONMENT || "testnet",
    reconcilerId: process.env.RECONCILER_ID || "primary-001",
    audit: {
      directory: process.env.AUDIT_DIR || "audit-out",
      intervalMs: integerFromEnv("AUDIT_INTERVAL_MS", 60_000),
      artifactRecordLimit: integerFromEnv("AUDIT_ARTIFACT_LIMIT", 2_000),
      toleranceEpsilon: process.env.AUDIT_TOLERANCE_EPSILON || DEFAULT_POLICY.tolerance.epsilon,
      flapWindowMs: integerFromEnv("AUDIT_FLAP_WINDOW_MS", 120_000),
    },
  };
}

/** Stream ids to monitor; empty means the caller supplies its own list. */
export function getMonitoredStreamIds(): string[] {
  const ids = process.env.STREAM_IDS;
  if (ids === undefined) return [];
  return ids
    .split(",")
    .map((id) => id.trim())
    .filter(Boolean);
}

export function auditPolicyFromConfig(config: RuntimeConfig): AuditPolicy {
  return { ...DEFAULT_POLICY, tolerance: { ...DEFAULT_POLICY.tolerance, epsilon: config.audit.toleranceEpsilon } };
}

export interface ReconcilerService {
  horizonReader: HorizonReader;
  sorobanReader: SorobanRpcReader;
  engine: ReconciliationEngine;
  alerting: AlertingService;
  auditor: ContinuousAuditor;
  sink: AuditSink;
  config: RuntimeConfig;
}

export function createReaders(config: RuntimeConfig): {
  horizonReader: HorizonReader;
  sorobanReader: SorobanRpcReader;
} {
  if (!config.horizonUrl) throw new Error("HORIZON_URL is required");
  if (!config.sorobanRpcUrl) throw new Error("SOROBAN_RPC_URL is required");

  const pollIntervalMs = config.pollIntervalMs;
  return {
    horizonReader: new HorizonReader({
      url: config.horizonUrl,
      token: process.env.HORIZON_API_TOKEN || undefined,
      pollIntervalMs,
    }),
    sorobanReader: new SorobanRpcReader({
      url: config.sorobanRpcUrl,
      token: process.env.SOROBAN_RPC_TOKEN || undefined,
      pollIntervalMs,
      contractId: config.contractId || "unknown",
    }),
  };
}

/**
 * Assembles the service. `onchain` is the contract read; `projected` defaults
 * to the Horizon-side projection the caller supplies, because the reconciler's
 * own cache is what is being audited.
 */
export async function createReconcilerService(
  config: RuntimeConfig = loadConfig(),
  sources?: { onchain: StateReader; projected: StateReader }
): Promise<ReconcilerService> {
  const { horizonReader, sorobanReader } = createReaders(config);

  const alerting = new AlertingService({
    webhookUrl: config.webhookUrl,
    consistencyWindowMs: config.consistencyWindowMs,
    logLevel: config.logLevel,
    serviceName: SERVICE_NAME,
    environment: config.environment,
    reconcilerId: config.reconcilerId,
  });

  const sink = new FileAuditSink(config.audit.directory, config.reconcilerId);
  const streamIds = getMonitoredStreamIds();

  // Without an injected pair, the contract read is the Soroban side and the
  // projection is empty: every record then shows up as missing on one side,
  // which is the honest report of "no projection has been built yet".
  const resolvedSources = sources ?? {
    onchain: {
      read: async () => ({ ledger: await sorobanReader.getLatestLedger(), records: [] }),
    },
    projected: projectionReader(async () => ({ ledger: 0, streams: [] as ProjectableStream[] })),
  };

  const auditor = new ContinuousAuditor(
    resolvedSources,
    sink,
    {
      intervalMs: config.audit.intervalMs,
      policy: auditPolicyFromConfig(config),
      artifactRecordLimit: config.audit.artifactRecordLimit,
      requireLedgerAdvance: true,
      flapWindowMs: config.audit.flapWindowMs,
    }
  );

  const engine = new ReconciliationEngine({
    consistencyWindowMs: config.consistencyWindowMs,
    alertingService: alerting,
  });

  const [horizonHealth, sorobanHealth] = await Promise.all([
    horizonReader.healthCheck(),
    sorobanReader.healthCheck(),
  ]);

  alerting.log("INFO", "Reconciler service starting", {
    horizonHealthy: horizonHealth.healthy,
    sorobanHealthy: sorobanHealth.healthy,
    monitoredStreams: streamIds.length,
    auditDirectory: config.audit.directory,
    auditIntervalMs: config.audit.intervalMs,
    toleranceEpsilon: config.audit.toleranceEpsilon,
    environment: config.environment,
    reconcilerId: config.reconcilerId,
  });

  return { horizonReader, sorobanReader, engine, alerting, auditor, sink, config };
}

/** One dual read plus one audit cycle — the shape the soak harness drives. */
export async function runCycle(
  horizonReader: HorizonReader,
  sorobanReader: SorobanRpcReader,
  engine: ReconciliationEngine,
  alerting: AlertingService,
  streamIds: string[]
): Promise<void> {
  const [horizonStates, sorobanStates] = await Promise.all([
    horizonReader.pollStreams(streamIds),
    sorobanReader.pollStreams(streamIds),
  ]);

  const result = await engine.reconcile(horizonStates, sorobanStates);

  alerting.log("INFO", "Reconciliation cycle complete", {
    totalChecked: result.totalChecked,
    inSync: result.inSync,
    withinLagWindow: result.withinLagWindow,
    divergences: result.divergences.length,
    ledgerGap: result.ledgerGap,
    timestamp: result.timestamp,
  });
}

const CLI_USAGE = `usage: node dist/index.js [command]

  audit --once        run a single merkleized audit cycle and write artifacts
  verify [options]    re-derive published roots and proofs (see --help below)
  (no command)        continuous reconciliation + audit loop

Environment: see .env.example. The audit writes report/bundle/artifact/checkpoint
files under AUDIT_DIR, named by RECONCILER_ID.`;

/** Reuses the verifier's flag grammar without spawning it. */
function parseVerifyArgs(argv: string[]): VerifyOptions {
  const options: VerifyOptions = {};
  for (let i = 0; i < argv.length; i++) {
    const flag = argv[i];
    const value = (): string => {
      const next = argv[i + 1];
      if (next === undefined) throw new Error(`${flag} needs a value`);
      i += 1;
      return next;
    };
    if (flag === "--bundle") options.bundlePath = value();
    else if (flag === "--report") options.reportPath = value();
    else if (flag === "--artifact") options.artifactPath = value();
    else if (flag === "--json") options.json = true;
    else throw new Error(`unknown verifier flag '${flag}'`);
  }
  return options;
}

export async function runCli(argv: string[]): Promise<number> {
  const command = argv[0];
  if (command === undefined) return -1;

  if (command === "--help" || command === "help") {
    process.stdout.write(`${CLI_USAGE}\n`);
    return 0;
  }
  if (command === "verify") {
    const result = await verify(parseVerifyArgs(argv.slice(1)));
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
    return result.ok ? 0 : 1;
  }
  if (command === "audit") {
    const once = argv.includes("--once");
    const config = loadConfig();
    const service = await createReconcilerService(config);
    const outcome = await service.auditor.runCycle();
    if (outcome.report === null) {
      process.stdout.write(`skipped: ${outcome.skipped}\n`);
      return 0;
    }
    const bundle =
      outcome.report.verdict === "clean"
        ? undefined
        : buildProofBundle(outcome.report, service.auditor.auditPolicy);
    const summary = bundle === undefined ? null : verifyBundle(bundle);
    process.stdout.write(
      `${JSON.stringify(
        {
          cycle: outcome.cycle,
          verdict: outcome.report.verdict,
          roots: outcome.report.roots,
          alerts: outcome.alerts,
          self_check_ok: summary === null ? true : summary.ok,
        },
        null,
        2
      )}\n`
    );
    if (!once) await service.auditor.run();
    return outcome.report.verdict === "fault" ? 1 : 0;
  }
  throw new Error(`unknown command '${command}'\n\n${CLI_USAGE}`);
}

async function mainLoop(): Promise<void> {
  const config = loadConfig();
  const service = await createReconcilerService(config);
  const streamIds = getMonitoredStreamIds();

  service.alerting.log("INFO", "Reconciler service ready", { monitoredStreams: streamIds.length });

  const dualRead = setInterval(() => {
    void runCycle(
      service.horizonReader,
      service.sorobanReader,
      service.engine,
      service.alerting,
      streamIds
    ).catch((error: unknown) => {
      service.alerting.log("ERROR", "Reconciliation cycle failed", {
        error: error instanceof Error ? error.message : String(error),
      });
    });
  }, config.consistencyWindowMs);

  try {
    await service.auditor.run();
  } finally {
    clearInterval(dualRead);
    service.auditor.stop();
  }
}

if (process.argv[2] !== undefined || (process.argv[1] !== undefined && process.argv[1].endsWith("index.js"))) {
  const code = await runCli(process.argv.slice(2));
  if (code === -1) {
    await mainLoop();
  } else {
    process.exitCode = code;
  }
}

export type { StreamState };
