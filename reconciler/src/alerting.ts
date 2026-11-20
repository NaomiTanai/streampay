//! Structured logging and webhook dispatch for the dual-read reconciliation
//! service.

import { createHttpAgent, type HttpAgent } from "./http.js";
import type { AlertingConfig } from "./types.js";
import type { DivergenceRecord } from "./types.js";

export type { AlertingConfig };

export interface WebhookDispatchResult {
  success: boolean;
  status?: number;
  message?: string;
  durationMs: number;
}

const LEVEL_ORDER: Record<AlertingConfig["logLevel"], number> = {
  DEBUG: 0,
  INFO: 1,
  WARN: 2,
  ERROR: 3,
};

export class AlertingService {
  private readonly config: AlertingConfig;
  private readonly webhookUrl?: string;
  private readonly http: HttpAgent;

  constructor(config: AlertingConfig, http: HttpAgent = createHttpAgent()) {
    this.config = config;
    this.webhookUrl = config.webhookUrl;
    this.http = http;
  }

  /** Emits a structured JSON log entry to stderr for log collection. */
  log(level: AlertingConfig["logLevel"], message: string, details: Record<string, unknown> = {}): void {
    if (!this.shouldLog(level)) return;

    const entry = {
      timestamp: new Date().toISOString(),
      service: this.config.serviceName,
      level,
      message,
      details,
    };
    process.stderr.write(`${JSON.stringify(entry)}\n`);
  }

  /** Dispatches a divergence alert to the configured webhook target. */
  async dispatchWebhook(divergence: DivergenceRecord): Promise<WebhookDispatchResult> {
    const start = Date.now();
    const payload = this.buildWebhookPayload(divergence);

    this.log("WARN", `Webhook dispatch initiated for stream ${divergence.streamId}`, {
      payloadSize: JSON.stringify(payload).length,
      checkType: divergence.checkType,
      severity: divergence.severity,
    });

    if (this.webhookUrl === undefined) {
      this.log("WARN", "No WEBHOOK_URL configured — skipping dispatch");
      return { success: false, message: "No webhook URL configured", durationMs: Date.now() - start };
    }

    try {
      const response = await this.http.post(this.webhookUrl, payload, {
        "User-Agent": `streampay-reconciler/${this.config.reconcilerId}`,
      });
      const success = response.status >= 200 && response.status < 300;
      this.log(success ? "INFO" : "WARN", `Webhook dispatch ${success ? "succeeded" : "failed"}`, {
        status: response.status,
        durationMs: Date.now() - start,
        streamId: divergence.streamId,
      });
      return {
        success,
        status: response.status,
        message: typeof response.data === "string" ? response.data : undefined,
        durationMs: Date.now() - start,
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.log("WARN", `Webhook dispatch failed: ${message}`, {
        durationMs: Date.now() - start,
        streamId: divergence.streamId,
        error: message,
      });
      return { success: false, message, durationMs: Date.now() - start };
    }
  }

  buildWebhookPayload(divergence: DivergenceRecord) {
    return {
      alert: {
        type: divergence.auditKind === undefined ? "stream_divergence" : "state_root_divergence",
        severity: divergence.severity,
        timestamp: divergence.detectedAt,
        source: "reconciler",
        data: {
          streamId: divergence.streamId,
          divergenceType: divergence.checkType,
          horizonState: divergence.indexedValue,
          sorobanState: divergence.onChainValue,
          lagMs: divergence.horizonLagMs ?? divergence.sorobanLagMs ?? 0,
          consequence: divergence.checkType === "missing_record" ? "index_corruption" : "balance_mismatch",
          ...(divergence.auditKind === undefined
            ? {}
            : { audit_kind: divergence.auditKind, audit_key: divergence.auditKey }),
        },
      },
      metadata: {
        version: "1.0.0",
        environment: this.config.environment,
        reconcilerId: this.config.reconcilerId,
      },
    };
  }

  private shouldLog(level: AlertingConfig["logLevel"]): boolean {
    return LEVEL_ORDER[level] >= LEVEL_ORDER[this.config.logLevel];
  }
}
