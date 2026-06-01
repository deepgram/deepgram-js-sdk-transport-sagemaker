# Changelog

## [0.1.1](https://github.com/deepgram/deepgram-js-sdk-transport-sagemaker/compare/v0.1.0...v0.1.1) (2026-06-01)

First published release of the SageMaker transport for the Deepgram JavaScript SDK. Routes the SDK's streaming connections through an AWS SageMaker bidirectional HTTP/2 endpoint via the SDK's `transportFactory` option, leaving the rest of the Deepgram client API unchanged.


### Features

* **transport:** SageMaker streaming transport via `createSageMakerTransportFactory(...)` — plug a SageMaker-hosted Deepgram model into `DeepgramClient` through the `transportFactory` option, covering the `listen.v1`, `listen.v2`, `speak.v1`, and `agent.v1` streaming paths. Includes jittered exponential-backoff retry with replay-buffer storm absorption, burst-tuned connection/subscription timeouts, configurable concurrency, and `KeepAlive` ping support ([4e21f8d](https://github.com/deepgram/deepgram-js-sdk-transport-sagemaker/commit/4e21f8ded975e7e64d45835d100650a2dd2b20dd))


### Bug Fixes

* **deps:** declare `@deepgram/sdk` as a non-optional `peerDependency` with a `>=5.4.0` floor (the release that added `transportFactory`), so consumers on an older SDK get an install-time peer warning instead of a confusing runtime failure ([#6](https://github.com/deepgram/deepgram-js-sdk-transport-sagemaker/pull/6)) ([3795ad2](https://github.com/deepgram/deepgram-js-sdk-transport-sagemaker/commit/3795ad215ce24dee2afd93facd2415743b44736e))

## 0.1.0 (2026-05-01)

### Features

- initial SageMaker transport for the Deepgram JavaScript SDK
