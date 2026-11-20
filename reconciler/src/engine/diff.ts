//! Core reconciliation engine: compares the Horizon and Soroban reads and
//! classifies each disagreement as in-sync, lag, or divergence.
//!
//! The merkleized audit (`src/audit/`) is the authoritative continuous
//! checker; this engine remains the per-stream dual read that explains *which*
//! field of a record a root divergence came from.

import type {
  AlertingConfig,
  StreamState,
  DivergenceRecord,
  ReconciliationResult,
} from "../types.js";

/** The part of the alerting service this engine depends on. */
export interface AlertTransport {
  log(level: AlertingConfig["logLevel"], message: string, details?: Record<string, unknown>): void;
  dispatchWebhook(divergence: DivergenceRecord): Promise<unknown>;
}

export interface ReconciliationEngineConfig {
  /** Acceptable propagation lag between the two reads. */
  consistencyWindowMs: number;
  alertingService: AlertTransport;
  /**
   * Optional de-duplication gate for divergence alerts (issue #492 policy).
   * Given the keys diverging this cycle it returns the ones worth paging on;
   * omit it to dispatch every critical divergence, as before.
   */
  alertGate?: (faultingKeys: string[], atMs: number) => string[];
}

interface ReaderLike {
  pollStreams(ids: string[]): Promise<StreamState[]>;
  healthCheck(): Promise<{ healthy: boolean; error?: string }>;
}

export class ReconciliationEngine {
  private readonly config: ReconciliationEngineConfig;
  private readonly alerting: AlertTransport;

  constructor(config: ReconciliationEngineConfig) {
    this.config = config;
    this.alerting = config.alertingService;
  }

  /** Reconciles two sets of stream states and detects divergences. */
  async reconcile(
    horizonStates: StreamState[],
    sorobanStates: StreamState[],
    atMs: number = Date.now()
  ): Promise<ReconciliationResult> {
    const startedAt = Date.now();

    const horizonMap = new Map(horizonStates.map((state) => [state.streamId, state]));
    const sorobanMap = new Map(sorobanStates.map((state) => [state.streamId, state]));
    const allStreamIds = new Set([
      ...horizonStates.map((state) => state.streamId),
      ...sorobanStates.map((state) => state.streamId),
    ]);

    const divergences: DivergenceRecord[] = [];
    let inSyncCount = 0;
    let withinLagCount = 0;

    for (const streamId of allStreamIds) {
      const horizonState = horizonMap.get(streamId);
      const sorobanState = sorobanMap.get(streamId);

      if (horizonState !== undefined && sorobanState !== undefined) {
        const result = this.compareStates(horizonState, sorobanState);
        if (result === null) {
          inSyncCount += 1;
        } else if (result === "lag") {
          withinLagCount += 1;
        } else {
          divergences.push(result);
        }
      } else if (horizonState !== undefined) {
        divergences.push(
          this.createDivergenceRecord(
            streamId,
            "missing_record",
            "critical",
            { streamId: horizonState.streamId, ledger: horizonState.ledger },
            { streamId: "N/A", ledger: 0 },
            horizonState.ledger,
            0
          )
        );
      } else if (sorobanState !== undefined) {
        divergences.push(
          this.createDivergenceRecord(
            streamId,
            "missing_record",
            "warning",
            { streamId: "N/A", ledger: 0 },
            { streamId: sorobanState.streamId, ledger: sorobanState.ledger },
            0,
            sorobanState.ledger
          )
        );
      }
    }

    const ledgerGap = this.calculateLedgerGap(horizonStates, sorobanStates);

    const critical = divergences.filter((divergence) => divergence.severity === "critical");
    const dispatchable =
      this.config.alertGate === undefined
        ? critical
        : (() => {
            const allowed = new Set(
              this.config.alertGate?.(critical.map((divergence) => divergence.streamId), atMs) ?? []
            );
            return critical.filter((divergence) => allowed.has(divergence.streamId));
          })();

    for (const divergence of dispatchable) {
      await this.alerting.dispatchWebhook(divergence);
    }

    this.alerting.log("INFO", "Reconciliation complete", {
      totalChecked: allStreamIds.size,
      inSync: inSyncCount,
      withinLagWindow: withinLagCount,
      divergences: divergences.length,
      dispatched: dispatchable.length,
      ledgerGap,
      durationMs: Date.now() - startedAt,
    });

    return {
      totalChecked: allStreamIds.size,
      inSync: inSyncCount,
      withinLagWindow: withinLagCount,
      divergences,
      timestamp: new Date(atMs).toISOString(),
      ledgerGap,
    };
  }

  /** `null` when in sync, `"lag"` inside the consistency window, else the record. */
  private compareStates(h: StreamState, s: StreamState): null | "lag" | DivergenceRecord {
    const depositMatch = h.data.deposit === s.data.deposit;
    const withdrawnMatch = h.data.withdrawn === s.data.withdrawn;
    const statusMatch = h.data.status === s.data.status;

    if (depositMatch && withdrawnMatch && statusMatch) return null;

    const timeDiffMs = Math.abs(
      new Date(h.timestamp).getTime() - new Date(s.timestamp).getTime()
    );

    // A disagreement the two reads can still be reconciling from is lag, not
    // a fault; the audit cycle re-checks it once the window closes.
    if (timeDiffMs <= this.config.consistencyWindowMs) {
      this.alerting.log("WARN", `Stream ${h.streamId} shows lag-induced divergence`, {
        horizonLedger: h.ledger,
        sorobanLedger: s.ledger,
        timeDiffMs,
        consistencyWindowMs: this.config.consistencyWindowMs,
        horizonData: h.data,
        sorobanData: s.data,
      });
      return "lag";
    }

    const severity: "warning" | "critical" =
      !depositMatch || !withdrawnMatch ? "critical" : "warning";

    return this.createDivergenceRecord(
      h.streamId,
      severity === "critical" ? "stream_balance" : "stream_status",
      severity,
      { streamId: h.streamId, ledger: h.ledger, timestamp: h.timestamp, data: h.data },
      { streamId: s.streamId, ledger: s.ledger, timestamp: s.timestamp, data: s.data },
      h.ledger,
      s.ledger
    );
  }

  private createDivergenceRecord(
    streamId: string,
    checkType: DivergenceRecord["checkType"],
    severity: DivergenceRecord["severity"],
    indexedValue: Record<string, unknown>,
    onChainValue: Record<string, unknown>,
    horizonLedger: number,
    sorobanLedger: number
  ): DivergenceRecord {
    const now = Date.now();
    return {
      streamId,
      checkType,
      severity,
      indexedValue,
      onChainValue,
      horizonLagMs: horizonLedger > 0 ? now - horizonLedger * 1000 : undefined,
      sorobanLagMs: sorobanLedger > 0 ? now - sorobanLedger * 1000 : undefined,
      detectedAt: new Date(now).toISOString(),
    };
  }

  private calculateLedgerGap(horizonStates: StreamState[], sorobanStates: StreamState[]): number {
    const latest = (states: StreamState[]): number =>
      states.length === 0 ? 0 : Math.max(...states.map((state) => state.ledger));
    return Math.abs(latest(horizonStates) - latest(sorobanStates));
  }

  /**
   * Runs reconciliation until `stop()` is called. Prefer `ContinuousAuditor`
   * for the merkleized path; this loop is retained for the per-stream read.
   */
  async startContinuousReconciliation(
    horizonReader: ReaderLike,
    sorobanReader: ReaderLike,
    streamIds: string[],
    options: {
      initialDelayMs?: number;
      healthCheckIntervalMs?: number;
      onError?: (error: unknown) => void;
      maxCycles?: number;
      shouldStop?: () => boolean;
    } = {}
  ): Promise<void> {
    const {
      initialDelayMs = 0,
      healthCheckIntervalMs = 30_000,
      onError = (error) => console.error("Reconciliation error:", error),
      maxCycles,
      shouldStop = () => false,
    } = options;

    if (initialDelayMs > 0) await sleep(initialDelayMs);

    let cycles = 0;
    const healthCheckLoop = setInterval(() => {
      void (async () => {
        try {
          const [horizonHealth, sorobanHealth] = await Promise.all([
            horizonReader.healthCheck(),
            sorobanReader.healthCheck(),
          ]);
          if (!horizonHealth.healthy || !sorobanHealth.healthy) {
            this.alerting.log("WARN", "One or more readers unhealthy", {
              horizonHealthy: horizonHealth.healthy,
              sorobanHealthy: sorobanHealth.healthy,
              horizonError: horizonHealth.error,
              sorobanError: sorobanHealth.error,
            });
          }
        } catch (error) {
          this.alerting.log("ERROR", "Health check failed", {
            error: error instanceof Error ? error.message : String(error),
          });
        }
      })();
    }, healthCheckIntervalMs);

    try {
      while (!shouldStop() && (maxCycles === undefined || cycles < maxCycles)) {
        const loopStartedAt = Date.now();
        try {
          const [horizonStates, sorobanStates] = await Promise.all([
            horizonReader.pollStreams(streamIds),
            sorobanReader.pollStreams(streamIds),
          ]);
          await this.reconcile(horizonStates, sorobanStates);
        } catch (error) {
          onError(error);
          await sleep(1_000);
        }
        cycles += 1;
        const remaining = Math.max(0, this.config.consistencyWindowMs - (Date.now() - loopStartedAt));
        if (remaining > 0 && !shouldStop()) await sleep(remaining);
      }
    } finally {
      clearInterval(healthCheckLoop);
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
