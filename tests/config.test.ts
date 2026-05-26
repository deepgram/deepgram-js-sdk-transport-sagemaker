import { describe, expect, it } from "vitest";

import {
  DEFAULT_BACKOFF_MULTIPLIER,
  DEFAULT_CONNECTION_TIMEOUT_MS,
  DEFAULT_INITIAL_BACKOFF_MS,
  DEFAULT_MAX_BACKOFF_MS,
  DEFAULT_MAX_CONCURRENCY,
  DEFAULT_MAX_REPLAY_BUFFER_BYTES,
  DEFAULT_MAX_RETRIES,
  DEFAULT_RETRY_BUDGET_MS,
  DEFAULT_SUBSCRIPTION_TIMEOUT_MS,
  resolveSageMakerConfig,
} from "../src/config";

describe("resolveSageMakerConfig", () => {
  it("applies burst-tuned defaults", () => {
    const cfg = resolveSageMakerConfig({ endpointName: "ep" });
    expect(cfg.connectionTimeoutMs).toBe(DEFAULT_CONNECTION_TIMEOUT_MS);
    expect(cfg.connectionTimeoutMs).toBe(30_000);
    expect(cfg.subscriptionTimeoutMs).toBe(DEFAULT_SUBSCRIPTION_TIMEOUT_MS);
    expect(cfg.subscriptionTimeoutMs).toBe(60_000);
    expect(cfg.maxConcurrency).toBe(DEFAULT_MAX_CONCURRENCY);
    expect(cfg.maxConcurrency).toBe(500);
    expect(cfg.maxRetries).toBe(DEFAULT_MAX_RETRIES);
    expect(cfg.maxRetries).toBe(5);
    expect(cfg.initialBackoffMs).toBe(DEFAULT_INITIAL_BACKOFF_MS);
    expect(cfg.initialBackoffMs).toBe(100);
    expect(cfg.maxBackoffMs).toBe(DEFAULT_MAX_BACKOFF_MS);
    expect(cfg.maxBackoffMs).toBe(5_000);
    expect(cfg.backoffMultiplier).toBe(DEFAULT_BACKOFF_MULTIPLIER);
    expect(cfg.backoffMultiplier).toBe(2.0);
    expect(cfg.retryBudgetMs).toBe(DEFAULT_RETRY_BUDGET_MS);
    expect(cfg.retryBudgetMs).toBe(30_000);
    expect(cfg.maxReplayBufferBytes).toBe(DEFAULT_MAX_REPLAY_BUFFER_BYTES);
    expect(cfg.maxReplayBufferBytes).toBe(8 * 1024 * 1024);
  });

  it("defaults region to us-west-2", () => {
    expect(resolveSageMakerConfig({ endpointName: "ep" }).region).toBe("us-west-2");
  });

  it("rejects blank endpointName", () => {
    expect(() => resolveSageMakerConfig({ endpointName: "   " })).toThrow(/endpointName is required/);
    expect(() => resolveSageMakerConfig({ endpointName: "" })).toThrow(/endpointName is required/);
  });

  it.each([
    "connectionTimeoutMs",
    "subscriptionTimeoutMs",
    "maxConcurrency",
    "initialBackoffMs",
    "maxBackoffMs",
    "retryBudgetMs",
  ] as const)("rejects non-positive %s", (field) => {
    expect(() => resolveSageMakerConfig({ endpointName: "ep", [field]: 0 })).toThrow(/positive/);
    expect(() => resolveSageMakerConfig({ endpointName: "ep", [field]: -1 })).toThrow(/positive/);
  });

  it("allows maxRetries=0 (disables internal retry)", () => {
    const cfg = resolveSageMakerConfig({ endpointName: "ep", maxRetries: 0 });
    expect(cfg.maxRetries).toBe(0);
  });

  it("rejects negative maxRetries", () => {
    expect(() => resolveSageMakerConfig({ endpointName: "ep", maxRetries: -1 })).toThrow(/non-negative/);
  });

  it("rejects backoffMultiplier < 1.0", () => {
    expect(() => resolveSageMakerConfig({ endpointName: "ep", backoffMultiplier: 0.5 })).toThrow(/must be >= 1.0/);
  });

  it("rejects initialBackoffMs > maxBackoffMs", () => {
    expect(() =>
      resolveSageMakerConfig({ endpointName: "ep", initialBackoffMs: 10_000, maxBackoffMs: 1_000 }),
    ).toThrow(/must not exceed maxBackoffMs/);
  });

  it("allows maxReplayBufferBytes=0 (disables replay)", () => {
    const cfg = resolveSageMakerConfig({ endpointName: "ep", maxReplayBufferBytes: 0 });
    expect(cfg.maxReplayBufferBytes).toBe(0);
  });

  it("rejects negative maxReplayBufferBytes", () => {
    expect(() => resolveSageMakerConfig({ endpointName: "ep", maxReplayBufferBytes: -1 })).toThrow(/non-negative/);
  });
});
