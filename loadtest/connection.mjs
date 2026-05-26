/**
 * A single streaming connection through the Deepgram JS SDK + SageMaker transport.
 *
 * Mirrors the Python (and Java) load-test connection class. The connection
 * goes through the full SDK adapter path (client.listen.v1.createConnection),
 * not the underlying transport directly, so the load test exercises the
 * same code path real users will integrate against.
 */

import process from "node:process";

// 8 KB chunks, matching the Java/Python harness.
const CHUNK_SIZE = 8192;

// If real-time pacing drifts more than this far behind, reset the baseline
// so the loop doesn't burst-send catch-up audio at line speed (which would
// overrun the model and truncate tail-end transcripts after CloseStream).
const PACING_DRIFT_THRESHOLD_MS = 1000;

// Default-retryable, narrow terminal allowlist. Matches the Python harness's
// classify() approach: the retry budget is the safety net, not the classifier.
// We only give up immediately on 4xx-coded caller-side rejections (auth,
// validation, missing endpoint) where retrying would just waste budget.
const TERMINAL_TOKENS = [
  "AccessDeniedException",
  "UnrecognizedClientException",
  "ValidationException",
  "InvalidEndpointException",
  "ResourceNotFoundException",
  "EndpointNotFoundException",
];

function isRetryable(message) {
  if (message == null) return true;
  return !TERMINAL_TOKENS.some((tok) => message.includes(tok));
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Returns a promise that resolves when either `ms` elapses OR the abort
// signal fires. Resolves with true if the abort fired, false otherwise.
function sleepInterruptible(ms, abortSignal) {
  return new Promise((resolve) => {
    if (abortSignal?.aborted) {
      resolve(true);
      return;
    }
    const timer = setTimeout(() => {
      abortSignal?.removeEventListener("abort", onAbort);
      resolve(false);
    }, ms);
    function onAbort() {
      clearTimeout(timer);
      resolve(true);
    }
    abortSignal?.addEventListener("abort", onAbort, { once: true });
  });
}

export class SdkStreamingConnection {
  constructor({
    connectionId,
    sdkClient,
    service, // "listen.v1" | "listen.v2"
    wavBuffer,
    wavInfo,
    connectOptions,
    loop,
    maxRetries,
    awaitFinalResultsMs,
    onFirstPassComplete,
  }) {
    this.connectionId = connectionId;
    this._sdkClient = sdkClient;
    this._service = service ?? "listen.v1";
    this._wavBuffer = wavBuffer;
    this._wavInfo = wavInfo;
    this._connectOptions = connectOptions;
    this._loop = loop;
    this._maxRetries = maxRetries;
    this._awaitFinalResultsMs = awaitFinalResultsMs;
    this._onFirstPassComplete = onFirstPassComplete;
    this._firstPassFired = false;

    // Internal stop signal — wraps an AbortController so sleeps and the
    // streaming loop can short-circuit cleanly.
    this._stopController = new AbortController();

    // Stats the dashboard and summary read.
    this.stats = {
      chunkCount: 0,
      transcriptCount: 0,
      retryCount: 0,
      errored: false,
      errorMessages: [],
      finalTranscripts: [],
      active: false,
      stopped: false,
      startTime: 0,
      endTime: 0,
    };

    const frames = Math.max(1, Math.floor(CHUNK_SIZE / wavInfo.bytesPerFrame));
    this._framesPerChunk = frames;
    this._bytesPerChunk = frames * wavInfo.bytesPerFrame;
    this._chunkDurationMs = (frames / wavInfo.sampleRate) * 1000;
  }

  stop() {
    this.stats.stopped = true;
    this._stopController.abort();
  }

  get _stopped() {
    return this.stats.stopped;
  }

  get _durationSeconds() {
    if (this.stats.startTime === 0) return 0;
    const end = this.stats.endTime || performance.now();
    return (end - this.stats.startTime) / 1000;
  }

  durationSeconds() {
    return this._durationSeconds;
  }

  // Build createConnection args from the harness's connectOptions. The
  // path field is harness-internal — drop it before handing to the SDK.
  _connectArgs() {
    const { path: _path, ...rest } = this._connectOptions;
    return rest;
  }

  async run() {
    this.stats.startTime = performance.now();
    this.stats.active = true;
    let attempt = 0;

    while (!this._stopped) {
      try {
        // Full SDK adapter path: createConnection → V1/V2Socket → adapter → transport.
        const socket =
          this._service === "listen.v2"
            ? await this._sdkClient.listen.v2.createConnection(this._connectArgs())
            : await this._sdkClient.listen.v1.createConnection(this._connectArgs());

        let streamErrored = false;
        const streamErrors = [];
        let closed = false;
        let closeResolve;
        const closePromise = new Promise((resolve) => {
          closeResolve = resolve;
        });

        socket.on("message", (message) => {
          // setupBinaryHandling parses JSON in the adapter path, so message
          // arrives as a parsed object for control / results and as a Buffer
          // for binary. listen.v1 doesn't emit binary.
          if (typeof message === "string") {
            try {
              const parsed = JSON.parse(message);
              this._handleResult(parsed);
            } catch {
              // ignore unparseable
            }
            return;
          }
          if (message && typeof message === "object") {
            this._handleResult(message);
          }
        });

        socket.on("error", (err) => {
          streamErrored = true;
          streamErrors.push(err?.message ?? String(err));
          if (!closed) {
            closed = true;
            closeResolve();
          }
        });

        socket.on("close", () => {
          if (!closed) {
            closed = true;
            closeResolve();
          }
        });

        socket.connect();
        await socket.waitForOpen();

        try {
          await this._streamAudio({
            sendMedia: (chunk) => socket.sendMedia(chunk),
          });
          if (!this._stopped && !closed) {
            try {
              socket.sendCloseStream({ type: "CloseStream" });
            } catch {
              // best-effort
            }
          }
          await Promise.race([closePromise, sleep(this._awaitFinalResultsMs)]);
        } finally {
          try {
            socket.close();
          } catch {
            // best-effort
          }
        }

        if (streamErrored) {
          // If we already captured at least one transcript, the error is a
          // tail-end "model idle after CloseStream" condition -- audio was
          // processed, transcript is good, no point retrying. Treat as success.
          if (this.stats.transcriptCount === 0) {
            throw new Error(streamErrors.join("; "));
          }
          process.stderr.write(
            `\n[Conn ${this.connectionId}] post-transcript error suppressed: ${streamErrors[0]?.slice(0, 100) ?? ""}\n`,
          );
        }

        this.stats.endTime = performance.now();
        if (!this._loop || this._stopped) {
          this.stats.active = false;
          return;
        }
        if (!this._firstPassFired) {
          this._firstPassFired = true;
          this._onFirstPassComplete?.();
        }
        attempt = 0;
      } catch (err) {
        if (this._stopped) {
          this.stats.active = false;
          return;
        }
        const msg = err?.message ?? String(err);
        if (isRetryable(msg) && attempt < this._maxRetries) {
          attempt += 1;
          this.stats.retryCount += 1;
          const backoffMs = Math.min(1000 * 2 ** (attempt - 1), 30_000);
          process.stderr.write(
            `\n[Conn ${this.connectionId}] retryable error (attempt ${attempt}/${this._maxRetries}), retrying in ${backoffMs}ms: ${msg.slice(0, 100)}\n`,
          );
          const aborted = await sleepInterruptible(backoffMs, this._stopController.signal);
          if (aborted) {
            this.stats.active = false;
            return;
          }
          continue;
        }
        this.stats.active = false;
        this.stats.errored = true;
        this.stats.errorMessages.push(msg);
        this.stats.endTime = performance.now();
        return;
      }
    }
    this.stats.active = false;
  }

  _handleResult(parsed) {
    if (!parsed) return;
    // listen.v1: final result via { type: "Results", is_final: true, channel.alternatives[0].transcript }.
    // listen.v2 (Flux): completed turn via { type: "TurnInfo", event: "EndOfTurn", transcript }.
    if (this._service === "listen.v2") {
      if (parsed.type !== "TurnInfo") return;
      if (parsed.event !== "EndOfTurn") return;
      const transcript = parsed.transcript;
      if (transcript) {
        this.stats.transcriptCount += 1;
        this.stats.finalTranscripts.push(transcript);
      }
      return;
    }
    if (parsed.type !== "Results" || !parsed.is_final) return;
    const transcript = parsed.channel?.alternatives?.[0]?.transcript;
    if (transcript) {
      this.stats.transcriptCount += 1;
      this.stats.finalTranscripts.push(transcript);
    }
  }

  async _streamAudio(socket) {
    const wav = this._wavInfo;
    let totalChunks = this.stats.chunkCount;
    let passCount = 0;
    let streamStart = performance.now();
    let chunksAtPacingStart = totalChunks;

    // Flux V2 needs the WAV/RIFF header to auto-detect the audio format;
    // sending raw PCM fails with UNPARSABLE_CLIENT_MESSAGE. listen.v1 takes
    // the format from query params (encoding/sample_rate/channels), so we
    // strip the header to avoid the model treating header bytes as audio.
    const startOffset = this._service === "listen.v2" ? 0 : wav.dataOffset;
    const endOffset = this._service === "listen.v2"
      ? this._wavBuffer.length
      : wav.dataOffset + wav.dataSize;

    while (!this._stopped) {
      passCount += 1;
      let pos = startOffset;
      const end = endOffset;

      while (!this._stopped && pos < end) {
        const remaining = end - pos;
        const len = Math.min(this._bytesPerChunk, remaining);
        const chunk = this._wavBuffer.subarray(pos, pos + len);
        pos += len;

        socket.sendMedia(chunk);
        totalChunks += 1;
        this.stats.chunkCount = totalChunks;

        const elapsedMs = performance.now() - streamStart;
        const targetMs = (totalChunks - chunksAtPacingStart) * this._chunkDurationMs;
        let sleepMs = targetMs - elapsedMs;
        if (sleepMs < -PACING_DRIFT_THRESHOLD_MS) {
          process.stderr.write(
            `\n[Conn ${this.connectionId}] pacing drift ${Math.round(-sleepMs)}ms detected — resetting baseline\n`,
          );
          streamStart = performance.now();
          chunksAtPacingStart = totalChunks;
          sleepMs = 0;
        }
        if (sleepMs > 0) {
          const aborted = await sleepInterruptible(sleepMs, this._stopController.signal);
          if (aborted) return;
        }
      }

      if (!this._stopped && !this._firstPassFired) {
        this._firstPassFired = true;
        this._onFirstPassComplete?.();
      }

      if (!this._loop) break;
    }
  }
}

export function buildConnectOptions({ service, model, sampleRate, channels, interimResults }) {
  // createConnection args. Coerce to strings since query params on the SDK
  // expect strings and the underlying transport URL-encodes them as-is.
  if (service === "listen.v2") {
    // Flux V2 takes just the model name; audio format is inferred from
    // the WAV/RIFF header in the byte stream.
    return {
      model: model && model !== "nova-3" ? model : "flux-general-multi",
      path: "/v2/listen",
    };
  }
  return {
    model,
    encoding: "linear16",
    sample_rate: String(sampleRate),
    channels: String(channels),
    interim_results: interimResults ? "true" : "false",
    path: "/v1/listen",
  };
}
