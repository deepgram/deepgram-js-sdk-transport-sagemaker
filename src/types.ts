import type {
  InvokeEndpointWithBidirectionalStreamCommand,
  InvokeEndpointWithBidirectionalStreamCommandOutput,
  SageMakerRuntimeHTTP2ClientConfig,
} from "@aws-sdk/client-sagemaker-runtime-http2";

import type { DeepgramTransportFactory as BaseTransportFactory, DeepgramTransportRequest } from "./deepgram";
import type { SageMakerTransport } from "./transport";

export interface SageMakerConfig {
  endpointName: string;
  region?: string;
  targetVariant?: string;
  clientConfig?: Omit<SageMakerRuntimeHTTP2ClientConfig, "region">;
  /** Max time to wait for the underlying HTTP/2 connect / first response. Defaults to 30 s. */
  connectionTimeoutMs?: number;
  /** Max time to wait for the SageMaker bidi stream to open before failing a connect attempt. Defaults to 60 s. */
  subscriptionTimeoutMs?: number;
  /** Cap on simultaneous in-flight HTTP/2 streams. Advisory in JS today. Defaults to 500. */
  maxConcurrency?: number;
  /** Max retries on transient AWS errors per stream invocation. Set to 0 to disable. Defaults to 5. */
  maxRetries?: number;
  /** First backoff delay applied after the initial failure. Defaults to 100 ms. */
  initialBackoffMs?: number;
  /** Cap on per-attempt backoff delay regardless of multiplier. Defaults to 5 s. */
  maxBackoffMs?: number;
  /** Exponential growth factor between retry attempts. Must be >= 1.0. Defaults to 2.0. */
  backoffMultiplier?: number;
  /** Total wall-clock budget across all retry attempts before giving up. Defaults to 30 s. */
  retryBudgetMs?: number;
  /**
   * Cap on the in-memory replay buffer that holds sent-but-unacked stream events.
   * Set to 0 to disable replay (sent events are dropped on internal reset).
   * Defaults to 8 MiB.
   */
  maxReplayBufferBytes?: number;
}

export interface ResolvedSageMakerConfig {
  endpointName: string;
  region: string;
  targetVariant?: string;
  clientConfig?: Omit<SageMakerRuntimeHTTP2ClientConfig, "region">;
  connectionTimeoutMs: number;
  subscriptionTimeoutMs: number;
  maxConcurrency: number;
  maxRetries: number;
  initialBackoffMs: number;
  maxBackoffMs: number;
  backoffMultiplier: number;
  retryBudgetMs: number;
  maxReplayBufferBytes: number;
}

export interface SageMakerRuntimeHTTP2ClientLike {
  send(
    command: InvokeEndpointWithBidirectionalStreamCommand,
    options?: {
      abortSignal?: AbortSignal;
    },
  ): Promise<InvokeEndpointWithBidirectionalStreamCommandOutput>;
  destroy?(): void;
}

export interface SageMakerTransportFactory extends BaseTransportFactory {
  readonly config: Readonly<ResolvedSageMakerConfig>;
  readonly client: SageMakerRuntimeHTTP2ClientLike;
  create(url: string, headers: Record<string, string>, request: DeepgramTransportRequest): SageMakerTransport;
  destroy(): void;
}
