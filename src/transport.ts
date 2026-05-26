import {
  InvokeEndpointWithBidirectionalStreamCommand,
  type RequestPayloadPart,
  type RequestStreamEvent,
  type ResponsePayloadPart,
  type ResponseStreamEvent,
} from "@aws-sdk/client-sagemaker-runtime-http2";

import type {
  DeepgramTransport,
  DeepgramTransportCloseEvent,
  DeepgramTransportMessage,
  DeepgramTransportRequest,
} from "./deepgram";
import { AsyncQueue } from "./queue";
import type { ResolvedSageMakerConfig, SageMakerRuntimeHTTP2ClientLike } from "./types";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export type RetryClassification = "RETRYABLE" | "TERMINAL";

/**
 * Full-jitter exponential backoff. Pure function for testability.
 *
 * Without jitter, N streams failing simultaneously all compute the same
 * exponential delay and retry in lockstep, hammering the endpoint in waves.
 * Full jitter -- uniform in `[initialMs, ceiling]` -- spreads the retry load
 * continuously over the backoff window.
 */
function defaultRng(lo: number, hi: number): number {
  return lo + Math.random() * (hi - lo);
}

export function computeBackoff(
  initialMs: number,
  maxMs: number,
  multiplier: number,
  attempt: number,
  randomUniform: (lo: number, hi: number) => number = defaultRng,
): number {
  let scaled = initialMs * multiplier ** attempt;
  if (!Number.isFinite(scaled) || scaled > maxMs) {
    scaled = maxMs;
  }
  const ceiling = Math.max(initialMs, scaled);
  if (ceiling <= initialMs) {
    return ceiling;
  }
  return randomUniform(initialMs, ceiling);
}

function pickStatusCode(error: unknown): number | undefined {
  if (typeof error !== "object" || error === null) return undefined;
  const e = error as Record<string, unknown> & { $metadata?: { httpStatusCode?: number } };
  const meta = e.$metadata;
  if (meta && typeof meta.httpStatusCode === "number") return meta.httpStatusCode;
  for (const key of ["statusCode", "$statusCode", "httpStatusCode"] as const) {
    const v = e[key];
    if (typeof v === "number") return v;
  }
  return undefined;
}

function pickErrorCode(error: unknown): string | undefined {
  if (typeof error !== "object" || error === null) return undefined;
  const e = error as Record<string, unknown>;
  for (const key of ["name", "errorCode", "code", "Code"] as const) {
    const v = e[key];
    if (typeof v === "string") return v;
  }
  return undefined;
}

/**
 * Classify an exception as RETRYABLE (transient) or TERMINAL.
 *
 * Default is RETRYABLE -- the retry budget is the safety net, not the
 * classifier. The narrow set of TERMINAL errors is caller-side rejections
 * from AWS: 4xx status codes other than 429 (throttling) and 424 (Failed
 * Dependency -- SageMaker upstream model error, often transient under
 * burst load). Anything throttling-coded is RETRYABLE regardless of status.
 * The cause chain is walked; unknown types default to RETRYABLE.
 */
export function classifyError(error: unknown): RetryClassification {
  const seen = new Set<unknown>();
  let cur: unknown = error;
  while (cur !== undefined && cur !== null && !seen.has(cur)) {
    seen.add(cur);
    const code = pickErrorCode(cur);
    if (code && /throttl/i.test(code)) {
      return "RETRYABLE";
    }
    const status = pickStatusCode(cur);
    if (status !== undefined && status >= 400 && status < 500 && status !== 429 && status !== 424) {
      return "TERMINAL";
    }
    cur = (cur as { cause?: unknown }).cause;
  }
  return "RETRYABLE";
}

function normalizeError(error: unknown): Error {
  if (error instanceof Error) {
    return error;
  }

  return new Error(String(error));
}

function isBlob(value: DeepgramTransportMessage): value is Blob {
  return typeof Blob !== "undefined" && value instanceof Blob;
}

function looksLikeJson(bytes: Uint8Array): boolean {
  return bytes.length > 1 && bytes[0] === 0x7b && bytes[1] === 0x22;
}

async function toUint8Array(message: DeepgramTransportMessage): Promise<Uint8Array> {
  if (typeof message === "string") {
    return textEncoder.encode(message);
  }

  if (message instanceof ArrayBuffer) {
    return new Uint8Array(message);
  }

  if (ArrayBuffer.isView(message)) {
    return new Uint8Array(message.buffer, message.byteOffset, message.byteLength);
  }

  if (isBlob(message)) {
    return new Uint8Array(await message.arrayBuffer());
  }

  return new Uint8Array(message);
}

function toTransportMessage(part: ResponsePayloadPart): DeepgramTransportMessage {
  const bytes = part.Bytes ?? new Uint8Array();

  if (part.DataType === "UTF8" || looksLikeJson(bytes)) {
    return textDecoder.decode(bytes);
  }

  return bytes;
}

function isCloseOnlyJson(text: string): boolean {
  return text.includes('"type":"Metadata"') || text.includes('"type":"Error"');
}

interface BufferedEvent {
  event: RequestStreamEvent;
  bytes: number;
}

export class SageMakerTransport implements DeepgramTransport {
  private requestStream: AsyncQueue<RequestStreamEvent> | undefined;
  private readonly messageListeners = new Set<(message: DeepgramTransportMessage) => void>();
  private readonly errorListeners = new Set<(error: Error) => void>();
  private readonly closeListeners = new Set<(event: DeepgramTransportCloseEvent) => void>();

  private connectPromise: Promise<void> | undefined;
  private closeEvent: DeepgramTransportCloseEvent | undefined;
  private closed = false;
  private closeSent = false;
  private readonly abortHandler?: () => void;

  // Events queued before the next connection lands (e.g. during a reset window).
  // Drained onto the new requestStream when _doConnect runs.
  private readonly pending: RequestStreamEvent[] = [];

  // Replay buffer: events sent on the current stream that AWS hasn't acked yet.
  // On internal reset, the next _doConnect drains this onto the new stream so
  // audio sent on the rejected stream isn't lost. Trimmed when real downstream
  // data arrives; capped at config.maxReplayBufferBytes with FIFO eviction.
  private readonly replay: BufferedEvent[] = [];
  private replayBytes = 0;

  // Retry budget tracking. Reset to 0 once real downstream data flows back to
  // the application -- NOT on connect success. Connect success only proves
  // TLS+HTTP/2 setup; the actual bidi-stream request can still be throttled.
  private retryAttempt = 0;
  private retryWindowStart = 0;
  private retryNotBefore = 0;

  public constructor(
    private readonly args: {
      client: SageMakerRuntimeHTTP2ClientLike;
      config: ResolvedSageMakerConfig;
      invocationPath: string;
      queryString: string;
      service: DeepgramTransportRequest["service"];
      abortSignal?: AbortSignal;
    },
  ) {
    if (args.abortSignal) {
      this.abortHandler = () => {
        this.close(1000, "aborted");
      };

      if (args.abortSignal.aborted) {
        this.abortHandler();
      } else {
        args.abortSignal.addEventListener("abort", this.abortHandler, { once: true });
      }
    }
  }

  public isOpen(): boolean {
    return !this.closed;
  }

  public onOpen(listener: () => void): void {
    if (!this.closed) {
      queueMicrotask(() => {
        if (!this.closed) {
          listener();
        }
      });
    }
  }

  public onMessage(listener: (message: DeepgramTransportMessage) => void): void {
    this.messageListeners.add(listener);
  }

  public onError(listener: (error: Error) => void): void {
    this.errorListeners.add(listener);
  }

  public onClose(listener: (event: DeepgramTransportCloseEvent) => void): void {
    this.closeListeners.add(listener);

    if (this.closeEvent) {
      queueMicrotask(() => {
        listener(this.closeEvent!);
      });
    }
  }

  public async send(message: DeepgramTransportMessage): Promise<void> {
    if (this.closed) {
      this.fail(new Error("Transport is closed"));
      return;
    }

    let payload: RequestPayloadPart;
    let byteLength: number;
    try {
      payload = await this.toRequestPayloadPart(message);
      byteLength = payload.Bytes?.byteLength ?? 0;
    } catch (error) {
      this.fail(error);
      return;
    }

    // Track close signals so the model's idle timeout after CloseStream is
    // treated as a normal close rather than a stream error to retry.
    if (typeof message === "string" && (message.includes('"type":"CloseStream"') || message.includes('"type":"Finalize"'))) {
      this.closeSent = true;
    }

    const event: RequestStreamEvent = { PayloadPart: payload };

    while (!this.closed) {
      try {
        // If the request stream exists (created by doConnect even before
        // client.send has returned), push directly. The queue buffers values
        // that arrive before AWS SDK starts iterating, so order is preserved.
        if (this.requestStream) {
          this.requestStream.push(event);
          this.bufferForReplay(event, byteLength);
          return;
        }
        // First connect: buffer the event in `pending` so doConnect drains
        // it onto the new stream BEFORE calling client.send(). Without this,
        // AWS SDK's first iteration of the request Body races ahead of any
        // push() into the queue: the SDK calls next() on an empty queue,
        // sends HTTP/2 headers with no payload, the SageMaker model sees
        // zero audio and emits an empty Metadata + ModelStreamError, and the
        // bidi stream is torn down before our first chunk lands.
        this.pending.push(event);
        await this.ensureConnected();
        if (this.closed) return;
        this.bufferForReplay(event, byteLength);
        return;
      } catch (error) {
        if (!this.handleRetryableError(error)) {
          this.fail(error);
          return;
        }
      }
    }
  }

  public ping(): void {
    if (this.args.service === "listen.v1" || this.args.service === "listen.v2" || this.args.service === "agent.v1") {
      void this.send('{"type":"KeepAlive"}');
      return;
    }

    throw new Error(`Ping is not supported for ${this.args.service}`);
  }

  public close(code = 1000, reason = "Normal"): void {
    if (this.closed) {
      return;
    }

    this.closed = true;
    // Drop anything queued during a reset window and free the replay buffer so
    // the transport doesn't pin its memory after close.
    this.pending.length = 0;
    this.clearReplay();
    this.requestStream?.close();
    this.requestStream = undefined;
    this.cleanup();
    this.emitClose({ code, reason });
  }

  private async ensureConnected(): Promise<void> {
    if (this.closed) {
      throw new Error("Transport is closed");
    }

    if (this.requestStream) {
      return;
    }

    if (!this.connectPromise) {
      this.connectPromise = this.doConnect().finally(() => {
        this.connectPromise = undefined;
      });
    }

    await this.connectPromise;
  }

  private async doConnect(): Promise<void> {
    // Honor a previously-scheduled backoff so subsequent reconnects pace out
    // rather than hammering the AWS frontline.
    const now = Date.now();
    if (this.retryNotBefore > now) {
      const sleepMs = this.retryNotBefore - now;
      await new Promise<void>((resolve) => setTimeout(resolve, sleepMs));
    }

    const stream = new AsyncQueue<RequestStreamEvent>();
    this.requestStream = stream;

    // Replay any unacked events from a prior failed stream attempt. They go
    // through the queue and are delivered to AWS in order, before any new
    // chunks the caller sends after this attempt succeeds.
    for (const buffered of this.replay) {
      stream.push(buffered.event);
    }
    while (this.pending.length > 0) {
      stream.push(this.pending.shift()!);
    }

    const command = new InvokeEndpointWithBidirectionalStreamCommand({
      EndpointName: this.args.config.endpointName,
      Body: stream,
      ModelInvocationPath: this.args.invocationPath,
      ModelQueryString: this.args.queryString || undefined,
      TargetVariant: this.args.config.targetVariant,
    });

    // Wrap client.send in a subscription-timeout AbortController so a stuck
    // bidi-stream open doesn't block sends forever.
    const timeoutController = new AbortController();
    const timer = setTimeout(() => {
      timeoutController.abort(new Error(`Subscription timed out after ${this.args.config.subscriptionTimeoutMs} ms`));
    }, this.args.config.subscriptionTimeoutMs);

    let response: Awaited<ReturnType<SageMakerRuntimeHTTP2ClientLike["send"]>>;
    try {
      response = await this.args.client.send(command, {
        abortSignal: this.composeAbortSignal(timeoutController.signal),
      });
    } catch (error) {
      stream.close();
      this.requestStream = undefined;
      throw error;
    } finally {
      clearTimeout(timer);
    }

    if (this.closed) {
      stream.close();
      this.requestStream = undefined;
      return;
    }

    void this.consumeResponses(response.Body, stream);
  }

  private composeAbortSignal(timeoutSignal: AbortSignal): AbortSignal {
    if (!this.args.abortSignal) {
      return timeoutSignal;
    }
    // Spec AbortSignal.any if available, otherwise compose manually.
    const anyOf = (AbortSignal as unknown as { any?: (signals: AbortSignal[]) => AbortSignal }).any;
    if (typeof anyOf === "function") {
      return anyOf([this.args.abortSignal, timeoutSignal]);
    }
    const ctrl = new AbortController();
    const trip = (reason: unknown) => ctrl.abort(reason);
    if (this.args.abortSignal.aborted) trip(this.args.abortSignal.reason);
    else this.args.abortSignal.addEventListener("abort", () => trip(this.args.abortSignal!.reason), { once: true });
    if (timeoutSignal.aborted) trip(timeoutSignal.reason);
    else timeoutSignal.addEventListener("abort", () => trip(timeoutSignal.reason), { once: true });
    return ctrl.signal;
  }

  private async consumeResponses(
    body: AsyncIterable<ResponseStreamEvent> | undefined,
    boundStream: AsyncQueue<RequestStreamEvent>,
  ): Promise<void> {
    if (!body) {
      this.close();
      return;
    }

    try {
      for await (const event of body) {
        // If the user requested close or this stream was superseded by a
        // reconnect, stop processing events from the old body.
        if (this.closed || this.requestStream !== boundStream) {
          return;
        }

        if ("PayloadPart" in event && event.PayloadPart) {
          const message = toTransportMessage(event.PayloadPart);
          this.handlePayloadAck(message);
          this.emitMessage(message);
          continue;
        }

        if ("ModelStreamError" in event && event.ModelStreamError) {
          throw new ModelStreamError(event.ModelStreamError.Message, event.ModelStreamError);
        }

        if ("InternalStreamFailure" in event && event.InternalStreamFailure) {
          throw new InternalStreamFailure(event.InternalStreamFailure.Message, event.InternalStreamFailure);
        }
      }

      // Stream ended normally.
      if (this.requestStream === boundStream) {
        this.close();
      }
    } catch (error) {
      if (this.closed || this.requestStream !== boundStream) {
        // Either the user closed us, or a more recent reconnect superseded
        // this body; the new path is responsible for surfacing errors.
        return;
      }
      // Treat closeSent (model idle timeout after CloseStream) as a normal close.
      if (this.closeSent) {
        this.close(1000, "Normal");
        return;
      }
      if (this.handleRetryableError(error)) {
        // Stream dropped; the next send will reconnect and replay buffered audio.
        return;
      }
      this.fail(error);
    }
  }

  private async toRequestPayloadPart(message: DeepgramTransportMessage): Promise<RequestPayloadPart> {
    const bytes = await toUint8Array(message);

    if (typeof message === "string") {
      return {
        Bytes: bytes,
        DataType: "UTF8",
      };
    }

    return {
      Bytes: bytes,
      DataType: "BINARY",
    };
  }

  private bufferForReplay(event: RequestStreamEvent, bytes: number): void {
    const cap = this.args.config.maxReplayBufferBytes;
    if (cap === 0) return;
    this.replay.push({ event, bytes });
    this.replayBytes += bytes;
    while (this.replayBytes > cap && this.replay.length > 0) {
      const dropped = this.replay.shift()!;
      this.replayBytes -= dropped.bytes;
    }
  }

  private clearReplay(): void {
    this.replay.length = 0;
    this.replayBytes = 0;
  }

  private handlePayloadAck(message: DeepgramTransportMessage): void {
    // Anything except Metadata/Error JSON counts as "the model consumed input"
    // -- it's safe to reset the retry budget and drop the replay buffer.
    // Trusting Metadata as an ack causes front-loss when the model errored
    // before producing transcript and the buffer is cleared prematurely.
    const isCloseOnly = typeof message === "string" && isCloseOnlyJson(message);
    if (isCloseOnly) {
      return;
    }
    if (this.retryAttempt !== 0 || this.retryWindowStart !== 0 || this.retryNotBefore !== 0) {
      this.retryAttempt = 0;
      this.retryWindowStart = 0;
      this.retryNotBefore = 0;
    }
    this.clearReplay();
  }

  /**
   * Process a runtime error and decide whether to reset the stream.
   *
   * Returns true if the error was RETRYABLE and budget remains -- caller drops
   * the current stream, the next operation reconnects. Returns false if the
   * error is TERMINAL or budget exhausted -- caller should surface the error.
   */
  private handleRetryableError(error: unknown): boolean {
    if (this.closed) return false;
    if (this.closeSent) return false;

    const cls = classifyError(error);
    const elapsed = this.retryWindowStart === 0 ? 0 : Date.now() - this.retryWindowStart;
    const budgetLeft = this.retryAttempt < this.args.config.maxRetries && elapsed < this.args.config.retryBudgetMs;
    if (cls !== "RETRYABLE" || !budgetLeft) {
      return false;
    }

    if (this.retryWindowStart === 0) {
      this.retryWindowStart = Date.now();
    }
    const attemptForBackoff = this.retryAttempt;
    this.retryAttempt += 1;
    const backoff = computeBackoff(
      this.args.config.initialBackoffMs,
      this.args.config.maxBackoffMs,
      this.args.config.backoffMultiplier,
      attemptForBackoff,
    );
    this.retryNotBefore = Date.now() + backoff;
    this.requestStream?.close();
    this.requestStream = undefined;
    return true;
  }

  private emitMessage(message: DeepgramTransportMessage): void {
    for (const listener of this.messageListeners) {
      listener(message);
    }
  }

  private emitError(error: Error): void {
    for (const listener of this.errorListeners) {
      listener(error);
    }
  }

  private emitClose(event: DeepgramTransportCloseEvent): void {
    if (this.closeEvent) {
      return;
    }

    this.cleanup();
    this.closeEvent = event;

    for (const listener of this.closeListeners) {
      listener(event);
    }
  }

  private fail(error: unknown): void {
    const normalizedError = normalizeError(error);

    if (!this.closed) {
      this.closed = true;
      this.pending.length = 0;
      this.clearReplay();
      this.requestStream?.close();
      this.requestStream = undefined;
    }

    this.cleanup();
    this.emitError(normalizedError);
    this.emitClose({ code: 1011, reason: normalizedError.message });
  }

  private cleanup(): void {
    if (this.args.abortSignal && this.abortHandler) {
      this.args.abortSignal.removeEventListener("abort", this.abortHandler);
    }
  }

  /** @internal test-only accessor */
  public _replayBufferSize(): number {
    return this.replay.length;
  }

  /** @internal test-only accessor */
  public _replayBufferBytes(): number {
    return this.replayBytes;
  }

  /** @internal test-only accessor */
  public _bufferForReplayForTest(event: RequestStreamEvent, bytes: number): void {
    this.bufferForReplay(event, bytes);
  }

  /** @internal test-only accessor */
  public _clearReplayForTest(): void {
    this.clearReplay();
  }

  /** @internal test-only accessor */
  public _handlePayloadAckForTest(message: DeepgramTransportMessage): void {
    this.handlePayloadAck(message);
  }

  /** @internal test-only accessor: set retry-tracking state to simulate a prior failure. */
  public _setRetryStateForTest(attempt: number, windowStart: number, notBefore: number): void {
    this.retryAttempt = attempt;
    this.retryWindowStart = windowStart;
    this.retryNotBefore = notBefore;
  }

  /** @internal test-only accessor */
  public _getRetryAttemptForTest(): number {
    return this.retryAttempt;
  }
}

class ModelStreamError extends Error {
  public readonly $metadata?: { httpStatusCode?: number };
  public readonly details: unknown;
  constructor(message: string | undefined, details: unknown) {
    super(message || "SageMaker model stream error");
    this.name = "ModelStreamError";
    this.details = details;
  }
}

class InternalStreamFailure extends Error {
  public readonly $metadata?: { httpStatusCode?: number };
  public readonly details: unknown;
  constructor(message: string | undefined, details: unknown) {
    super(message || "SageMaker internal stream failure");
    this.name = "InternalStreamFailure";
    this.details = details;
  }
}
