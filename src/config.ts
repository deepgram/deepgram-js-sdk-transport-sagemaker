/**
 * Configuration defaults and validation for the SageMaker transport.
 *
 * Defaults are tuned for high-burst workloads (large numbers of streams opened
 * in a tight loop against an endpoint that may need to scale up). They are
 * intentionally more lenient than the AWS SDK defaults so that 200--500-stream
 * bursts don't trip connect-handshake / subscription-wait timeouts before the
 * endpoint has accepted the inbound TLS handshakes. Tighten them if you want
 * fail-fast behavior in low-latency pipelines.
 */

import type { ResolvedSageMakerConfig, SageMakerConfig } from "./types";

/** AWS SDK's underlying HTTP/2 connect is ~2s by default. Bumped so cold endpoints under burst load can accept TLS. */
export const DEFAULT_CONNECTION_TIMEOUT_MS = 30_000;

/** Time to wait for the SageMaker bidi stream to open before failing the first send. */
export const DEFAULT_SUBSCRIPTION_TIMEOUT_MS = 60_000;

/**
 * Cap on simultaneous in-flight HTTP/2 streams. Advisory in JS today — the
 * AWS SDK v3 HTTP/2 client doesn't expose a hard cap — but kept for API
 * parity and to feed any future Node-side concurrency limiter.
 */
export const DEFAULT_MAX_CONCURRENCY = 500;

/** Max retries on transient AWS errors per stream invocation. Terminal errors bypass this. */
export const DEFAULT_MAX_RETRIES = 5;

/** First backoff delay after the initial failure. */
export const DEFAULT_INITIAL_BACKOFF_MS = 100;

/** Cap on per-attempt backoff delay regardless of multiplier. */
export const DEFAULT_MAX_BACKOFF_MS = 5_000;

/** Exponential growth factor between retry attempts. Must be >= 1.0. */
export const DEFAULT_BACKOFF_MULTIPLIER = 2.0;

/** Total wall-clock budget across all retry attempts before giving up. */
export const DEFAULT_RETRY_BUDGET_MS = 30_000;

/**
 * Cap on the in-memory replay buffer that holds sent-but-unacked stream events
 * for the current bidi stream attempt. On internal reset, the buffer is drained
 * onto the new stream so AWS sees a continuous audio sequence rather than the
 * gap created by the discarded events. Trimmed when a real downstream payload
 * arrives. 8 MiB ~= 256s of 16 kHz mono 16-bit PCM.
 */
export const DEFAULT_MAX_REPLAY_BUFFER_BYTES = 8 * 1024 * 1024;

function requirePositive(name: string, value: number | undefined): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`${name} must be a positive finite number`);
  }
  return value;
}

function requireNonNegative(name: string, value: number | undefined): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`${name} must be a non-negative finite number`);
  }
  return value;
}

export function resolveSageMakerConfig(config: SageMakerConfig): ResolvedSageMakerConfig {
  if (!config.endpointName || !config.endpointName.trim()) {
    throw new Error("endpointName is required");
  }

  const connectionTimeoutMs =
    requirePositive("connectionTimeoutMs", config.connectionTimeoutMs) ?? DEFAULT_CONNECTION_TIMEOUT_MS;
  const subscriptionTimeoutMs =
    requirePositive("subscriptionTimeoutMs", config.subscriptionTimeoutMs) ?? DEFAULT_SUBSCRIPTION_TIMEOUT_MS;
  const maxConcurrency = requirePositive("maxConcurrency", config.maxConcurrency) ?? DEFAULT_MAX_CONCURRENCY;
  const maxRetries = requireNonNegative("maxRetries", config.maxRetries) ?? DEFAULT_MAX_RETRIES;
  const initialBackoffMs =
    requirePositive("initialBackoffMs", config.initialBackoffMs) ?? DEFAULT_INITIAL_BACKOFF_MS;
  const maxBackoffMs = requirePositive("maxBackoffMs", config.maxBackoffMs) ?? DEFAULT_MAX_BACKOFF_MS;
  const backoffMultiplier = config.backoffMultiplier ?? DEFAULT_BACKOFF_MULTIPLIER;
  if (!Number.isFinite(backoffMultiplier) || backoffMultiplier < 1.0) {
    throw new Error("backoffMultiplier must be >= 1.0");
  }
  const retryBudgetMs = requirePositive("retryBudgetMs", config.retryBudgetMs) ?? DEFAULT_RETRY_BUDGET_MS;
  const maxReplayBufferBytes =
    requireNonNegative("maxReplayBufferBytes", config.maxReplayBufferBytes) ?? DEFAULT_MAX_REPLAY_BUFFER_BYTES;

  if (initialBackoffMs > maxBackoffMs) {
    throw new Error(
      `initialBackoffMs (${initialBackoffMs}) must not exceed maxBackoffMs (${maxBackoffMs})`,
    );
  }

  return {
    endpointName: config.endpointName.trim(),
    region: config.region?.trim() || "us-west-2",
    targetVariant: config.targetVariant?.trim() || undefined,
    clientConfig: config.clientConfig,
    connectionTimeoutMs,
    subscriptionTimeoutMs,
    maxConcurrency,
    maxRetries,
    initialBackoffMs,
    maxBackoffMs,
    backoffMultiplier,
    retryBudgetMs,
    maxReplayBufferBytes,
  };
}
