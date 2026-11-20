//! Horizon reader: polls classic-bridge effects for stream state.

import { createHttpAgent, type HttpAgent } from "../http.js";
import type { StreamState, StreamStatus } from "../types.js";

export interface HorizonReaderConfig {
  url: string;
  token?: string;
  pollIntervalMs: number;
  contractAccount?: string;
}

export interface HorizonHealth {
  healthy: boolean;
  lastLedger?: number;
  error?: string;
}

export class HorizonReader {
  private readonly config: HorizonReaderConfig;
  private readonly http: HttpAgent;
  private lastLedger = 0;
  private healthy = false;

  constructor(config: HorizonReaderConfig, http: HttpAgent = createHttpAgent()) {
    this.config = config;
    this.http = http;
  }

  async healthCheck(): Promise<HorizonHealth> {
    try {
      const response = await this.http.get(`${this.config.url}/`);
      this.healthy = response.status === 200;
      return { healthy: this.healthy, lastLedger: this.lastLedger };
    } catch (error) {
      this.healthy = false;
      return {
        healthy: false,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  /**
   * Current known state for each stream id. The Horizon effect projection is
   * still a placeholder: it reports zeros rather than replaying effects, so a
   * dual read against live contract state will disagree. That is exactly why
   * the merkleized audit treats this side as a projection, not as truth.
   */
  async pollStreams(streamIds: string[]): Promise<StreamState[]> {
    if (streamIds.length === 0) return [];
    try {
      return streamIds.map((id) => ({
        streamId: id,
        ledger: this.lastLedger,
        timestamp: new Date().toISOString(),
        data: { deposit: "0", withdrawn: "0", status: "Active" satisfies StreamStatus },
      }));
    } catch (error) {
      this.healthy = false;
      console.warn(`Horizon reader error: ${String(error)}`);
      return [];
    }
  }

  /** Historical effects for one stream from a ledger onwards. */
  async getHistoricalData(_streamId: string, _fromLedger: number): Promise<StreamState[]> {
    return [];
  }

  async updateLastLedger(): Promise<number> {
    try {
      const response = await this.http.get(`${this.config.url}/ledgers?limit=1&order=desc`);
      const data = response.data as { _embedded?: { records?: Array<{ sequence?: number }> } };
      const sequence = data?._embedded?.records?.[0]?.sequence;
      if (typeof sequence === "number") {
        this.lastLedger = sequence;
        this.healthy = true;
      }
    } catch {
      this.healthy = false;
    }
    return this.lastLedger;
  }

  get latestKnownLedger(): number {
    return this.lastLedger;
  }
}
