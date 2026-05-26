# Deepgram SageMaker Transport for JavaScript

[![Node.js 20+](https://img.shields.io/badge/node-20%2B-blue)](https://nodejs.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

SageMaker transport for the [Deepgram JavaScript SDK](https://github.com/deepgram/deepgram-js-sdk). It replaces the SDK's default streaming WebSocket transport with SageMaker's bidirectional HTTP/2 runtime API so the rest of the Deepgram client API stays the same.

## Status

This package targets the pluggable transport interface introduced in Deepgram JS SDK PR `#492`.

## Requirements

- Node.js 20+
- A Deepgram JS SDK build that includes `transportFactory` support
- AWS credentials configured for SageMaker access
- A Deepgram model deployed behind an AWS SageMaker endpoint

## Installation

```bash
npm install @deepgram/sdk @deepgram/sagemaker@0.1.0 # x-release-please-version
```

## Authentication

This transport uses AWS credentials, not Deepgram API keys. Authentication is handled by the AWS SDK credential chain, including:

1. Environment variables such as `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
2. Shared AWS config and credential files
3. IAM roles
4. Custom credentials passed through `clientConfig`

The `apiKey` passed to `DeepgramClient` is unused once this transport is configured, but the SDK still requires a value to construct the client.

## Quickstart

```ts
import { DeepgramClient } from "@deepgram/sdk";
import { createSageMakerTransportFactory } from "@deepgram/sagemaker";

const transportFactory = createSageMakerTransportFactory({
  endpointName: "my-deepgram-endpoint",
  region: "us-west-2",
});

const client = new DeepgramClient({
  apiKey: "unused",
  transportFactory,
});

const socket = await client.listen.v1.createConnection({
  model: "nova-3",
});

socket.on("message", (message) => {
  console.log(message);
});

socket.connect();
socket.sendMedia(new Uint8Array([1, 2, 3]));
```

The transport is transparent: your Deepgram SDK usage stays the same while the underlying stream is routed to SageMaker.

## Configuration

```ts
import type { SageMakerConfig } from "@deepgram/sagemaker";

const config: SageMakerConfig = {
  endpointName: "my-deepgram-endpoint",
  region: "us-west-2",
  targetVariant: "blue",
  clientConfig: {
    maxAttempts: 2,
  },
  // Burst-tuned timeouts and retry behavior (defaults shown):
  connectionTimeoutMs: 30_000,
  subscriptionTimeoutMs: 60_000,
  maxConcurrency: 500,
  maxRetries: 5,
  initialBackoffMs: 100,
  maxBackoffMs: 5_000,
  backoffMultiplier: 2.0,
  retryBudgetMs: 30_000,
  maxReplayBufferBytes: 8 * 1024 * 1024,
};
```

All time-based fields are in milliseconds.

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `endpointName` | Yes | none | SageMaker endpoint name |
| `region` | No | `us-west-2` | AWS region |
| `targetVariant` | No | none | Optional SageMaker target variant |
| `clientConfig` | No | none | Additional AWS client configuration |
| `connectionTimeoutMs` | No | `30_000` | Max time for the underlying HTTP/2 connect / first response (AWS default is ~2 s — bumped here so cold-start endpoints under burst load have time to accept TLS handshakes). |
| `subscriptionTimeoutMs` | No | `60_000` | Max time the transport waits for the SageMaker bidi stream to open before failing a connect attempt. A timeout is treated as a transient failure and counts against `maxRetries` / `retryBudgetMs`. |
| `maxConcurrency` | No | `500` | Cap on simultaneous in-flight HTTP/2 streams. Advisory in JS today — the AWS SDK v3 HTTP/2 client doesn't expose a hard cap, but the knob is kept for surface parity and any future Node-side concurrency limiter. |
| `maxRetries` | No | `5` | Max retries on transient AWS errors (throttling, transient connect/timeout). Set to `0` to disable internal retry. Terminal errors (auth, validation) bypass this. |
| `initialBackoffMs` | No | `100` | First backoff delay applied after the initial failure. |
| `maxBackoffMs` | No | `5_000` | Cap on per-attempt backoff delay regardless of multiplier. |
| `backoffMultiplier` | No | `2.0` | Exponential growth factor between retry attempts. Must be `>= 1.0`. |
| `retryBudgetMs` | No | `30_000` | Total wall-clock cap across all retry attempts before giving up. |
| `maxReplayBufferBytes` | No | `8 * 1024 * 1024` | Cap on the in-memory replay buffer that holds sent-but-unacked stream events. Set to `0` to disable replay (sent events are dropped on internal reset). |

### High-concurrency notes

The transport's defaults are tuned for high-burst workloads (large numbers of
streams opened in a tight loop against an endpoint that may need to scale up).
If you open 200–500 streams simultaneously against a cold endpoint, the AWS
SDK's general-purpose defaults (~2 s connect) will fire before the load
balancer has accepted all of the inbound TLS handshakes — you'll see a wave
of connect / acquire timeouts that look like server-side problems but are
really client-side fail-fast tripping early.

Ship with the lenient defaults and tighten them only if you need fail-fast
behavior in low-latency pipelines:

```ts
const transportFactory = createSageMakerTransportFactory({
  endpointName: "my-deepgram-endpoint",
  region: "us-east-1",
  connectionTimeoutMs: 5_000,
  subscriptionTimeoutMs: 15_000,
});
```

### Retry & storm absorption

Transient AWS-side failures (`ThrottlingException`, transient connect/timeout
failures, `ModelError`/`424` from the upstream container) are absorbed by the
transport itself: classified as retryable, retried with jittered exponential
backoff up to `maxRetries` and `retryBudgetMs`, with messages buffered during
the reset window replayed onto the new stream so audio isn't dropped. Only
**terminal** errors (auth, validation, resource-not-found) and budget-exhausted
retryable errors propagate to the application.

```ts
const transportFactory = createSageMakerTransportFactory({
  endpointName: "my-deepgram-endpoint",
  maxRetries: 10,
  initialBackoffMs: 200,
  maxBackoffMs: 10_000,
  retryBudgetMs: 60_000,
});
```

Set `maxRetries: 0` to disable internal retry entirely (every transient AWS
error then surfaces immediately to the application).

When using this transport with the Deepgram JS SDK, also pass `reconnect: false`
on the `DeepgramClient` (or rely on the SDK's auto-disable when
`transportFactory` is set) so the SDK's wrapper-level retry layer doesn't
double-stack on top of this transport's internal retry.

## Custom AWS Client

If you already manage the SageMaker runtime client yourself, pass it as the second argument:

```ts
import { SageMakerRuntimeHTTP2Client } from "@aws-sdk/client-sagemaker-runtime-http2";
import { createSageMakerTransportFactory } from "@deepgram/sagemaker";

const awsClient = new SageMakerRuntimeHTTP2Client({
  region: "us-west-2",
});

const transportFactory = createSageMakerTransportFactory(
  { endpointName: "my-deepgram-endpoint" },
  awsClient,
);
```

The returned factory also exposes:

- `transportFactory.client`
- `transportFactory.config`
- `transportFactory.create(url, headers, request)`
- `transportFactory.destroy()`

For naming parity with the Java and Python packages, the package also exports `SageMakerTransportFactory` as an alias of `createSageMakerTransportFactory`.

## Keepalive Behavior

For `listen.v1`, `listen.v2`, and `agent.v1`, the transport implements `ping()` by sending a Deepgram `{"type":"KeepAlive"}` control message through SageMaker.

## How It Works

The Deepgram SDK still builds the same streaming request metadata it would normally use for a WebSocket connection. This package converts that request into SageMaker's `InvokeEndpointWithBidirectionalStream` API:

```text
Deepgram JS SDK -> transportFactory(url, headers, request)
                 -> SageMaker HTTP/2 bidirectional stream
                 -> your deployed Deepgram model
```

Audio and text control messages are written into the SageMaker request stream, and transcript or audio responses are surfaced back through the Deepgram transport interface.

## Development

```bash
npm install
npm run check
npm run pack:check
```

## Examples

This repo now includes the same five example categories as the Python and Java transport repos:

- `examples/stt.mjs`
- `examples/flux.mjs`
- `examples/live-mic.mjs`
- `examples/live-mic-flux.mjs`
- `examples/tts.mjs`

Run them with:

```bash
npm run example:stt
npm run example:flux
npm run example:live-mic
npm run example:live-mic-flux
npm run example:tts
```

Notes:

- Examples expect an `@deepgram/sdk` build with `transportFactory` support.
- Live microphone examples also require the optional `mic` package: `npm install mic`.
- File-based examples expect `spacewalk.wav` in the repo root by default, or `AUDIO_FILE` can point to another WAV file.

## License

MIT
