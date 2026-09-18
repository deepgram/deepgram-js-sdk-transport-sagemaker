# Changelog

## [0.1.2](https://github.com/deepgram/deepgram-js-sdk-transport-sagemaker/compare/v0.1.1...v0.1.2) (2026-09-18)

Updates the SageMaker transport to match the Deepgram JavaScript SDK transport contract. Existing applications using a supported SDK version need no code changes.

### Bug Fixes

* Require `@deepgram/sdk` `>=5.5.0 <6`, the supported range for the `transportFactory` interface. SDK `5.4.x` did not provide that interface and could not use this transport. ([b07274c](https://github.com/deepgram/deepgram-js-sdk-transport-sagemaker/commit/b07274cfc820f6ec347ff07ba559fa2d00096256))
* Align exported transport types with the SDK contract, preventing TypeScript compatibility errors when constructing `DeepgramClient` with `transportFactory`. ([7ecaf2c](https://github.com/deepgram/deepgram-js-sdk-transport-sagemaker/commit/7ecaf2c99fb2269be9bc4efc03942d0c698687c0))
* Add SageMaker transport compatibility for streaming Speak v2 requests, including graceful handling of the `Close` message. Speak v2 requires `@deepgram/sdk` `>=5.6.0`; a Flux TTS example is included. ([c4c31eb](https://github.com/deepgram/deepgram-js-sdk-transport-sagemaker/commit/c4c31eb84758440214145cc4bf2d461ca86fbbae))

## [0.1.1](https://github.com/deepgram/deepgram-js-sdk-transport-sagemaker/compare/v0.1.0...v0.1.1) (2026-06-01)

First published release of the SageMaker transport for the Deepgram JavaScript SDK. Routes the SDK's streaming connections through an AWS SageMaker bidirectional HTTP/2 endpoint via the SDK's `transportFactory` option, leaving the rest of the Deepgram client API unchanged.


### Features

* **transport:** SageMaker streaming transport via `createSageMakerTransportFactory(...)` — plug a SageMaker-hosted Deepgram model into `DeepgramClient` through the `transportFactory` option, covering the `listen.v1`, `listen.v2`, `speak.v1`, and `agent.v1` streaming paths. Includes jittered exponential-backoff retry with replay-buffer storm absorption, burst-tuned connection/subscription timeouts, configurable concurrency, and `KeepAlive` ping support ([4e21f8d](https://github.com/deepgram/deepgram-js-sdk-transport-sagemaker/commit/4e21f8ded975e7e64d45835d100650a2dd2b20dd))


### Bug Fixes

* **deps:** declare `@deepgram/sdk` as a non-optional `peerDependency` with a `>=5.4.0` floor (the release that added `transportFactory`), so consumers on an older SDK get an install-time peer warning instead of a confusing runtime failure ([#6](https://github.com/deepgram/deepgram-js-sdk-transport-sagemaker/pull/6)) ([3795ad2](https://github.com/deepgram/deepgram-js-sdk-transport-sagemaker/commit/3795ad215ce24dee2afd93facd2415743b44736e))

## 0.1.0 (2026-05-01)

### Features

- initial SageMaker transport for the Deepgram JavaScript SDK
