import type { RequestStreamEvent } from "@aws-sdk/client-sagemaker-runtime-http2";
import { describe, expect, it } from "vitest";

import { resolveSageMakerConfig } from "../src/config";
import { SageMakerTransport, classifyError, computeBackoff } from "../src/transport";
import type { SageMakerRuntimeHTTP2ClientLike } from "../src/types";

function payloadEvent(s: string): RequestStreamEvent {
  return { PayloadPart: { Bytes: new TextEncoder().encode(s), DataType: "UTF8" } };
}

function newTransport(maxReplayBufferBytes = 1024): SageMakerTransport {
  const client: SageMakerRuntimeHTTP2ClientLike = {
    send: () => {
      throw new Error("not used in this test");
    },
  };
  return new SageMakerTransport({
    client,
    config: resolveSageMakerConfig({ endpointName: "ep", maxReplayBufferBytes }),
    invocationPath: "v1/listen",
    queryString: "",
    service: "listen.v1",
  });
}

describe("classifyError", () => {
  it("treats TypeError-like timeouts as RETRYABLE", () => {
    const err = new Error("acquire timeout");
    expect(classifyError(err)).toBe("RETRYABLE");
  });

  it("treats AWS 429 throttling as RETRYABLE", () => {
    const err = Object.assign(new Error("Rate exceeded"), {
      name: "ThrottlingException",
      $metadata: { httpStatusCode: 429 },
    });
    expect(classifyError(err)).toBe("RETRYABLE");
  });

  it("treats AWS 5xx as RETRYABLE", () => {
    const err = Object.assign(new Error("internal"), {
      name: "InternalServerError",
      $metadata: { httpStatusCode: 503 },
    });
    expect(classifyError(err)).toBe("RETRYABLE");
  });

  it("treats throttling-coded errors as RETRYABLE regardless of status", () => {
    const err = Object.assign(new Error("Rate exceeded"), {
      name: "ThrottlingException",
      $metadata: { httpStatusCode: 400 },
    });
    expect(classifyError(err)).toBe("RETRYABLE");
  });

  it("treats AWS 401 as TERMINAL", () => {
    const err = Object.assign(new Error("Forbidden"), {
      name: "UnauthorizedException",
      $metadata: { httpStatusCode: 401 },
    });
    expect(classifyError(err)).toBe("TERMINAL");
  });

  it("treats AWS 403 as TERMINAL", () => {
    const err = Object.assign(new Error("Forbidden"), {
      name: "AccessDeniedException",
      $metadata: { httpStatusCode: 403 },
    });
    expect(classifyError(err)).toBe("TERMINAL");
  });

  it("treats AWS 400 ValidationException as TERMINAL", () => {
    const err = Object.assign(new Error("invalid input"), {
      name: "ValidationException",
      $metadata: { httpStatusCode: 400 },
    });
    expect(classifyError(err)).toBe("TERMINAL");
  });

  it("treats AWS 404 as TERMINAL", () => {
    const err = Object.assign(new Error("endpoint not found"), {
      name: "ResourceNotFoundException",
      $metadata: { httpStatusCode: 404 },
    });
    expect(classifyError(err)).toBe("TERMINAL");
  });

  it("treats AWS 424 ModelError as RETRYABLE", () => {
    const err = Object.assign(new Error("Failed to establish WebSocket connection"), {
      name: "ModelErrorException",
      $metadata: { httpStatusCode: 424 },
    });
    expect(classifyError(err)).toBe("RETRYABLE");
  });

  it("walks the cause chain", () => {
    const inner = Object.assign(new Error("netty"), {
      name: "ValidationException",
      $metadata: { httpStatusCode: 400 },
    });
    const wrapper = new Error("oops", { cause: inner });
    expect(classifyError(wrapper)).toBe("TERMINAL");
  });

  it("defaults unknown errors to RETRYABLE", () => {
    expect(classifyError(new Error("mystery"))).toBe("RETRYABLE");
  });

  it("treats throttling in the error name as RETRYABLE without an explicit code field", () => {
    class ThrottlingException extends Error {
      constructor() {
        super("Rate exceeded");
        this.name = "ThrottlingException";
      }
    }
    expect(classifyError(new ThrottlingException())).toBe("RETRYABLE");
  });
});

describe("computeBackoff", () => {
  const minRng = (lo: number, _hi: number) => lo;
  const maxRng = (_lo: number, hi: number) => hi;
  const midRng = (lo: number, hi: number) => lo + (hi - lo) / 2;

  it("returns the floor at attempt=0 (degenerate range)", () => {
    expect(computeBackoff(100, 1000, 2.0, 0, midRng)).toBe(100);
  });

  it("midpoint at attempt=1", () => {
    // initial=100, mult=2 -> ceiling=200. mid = 100 + 50 = 150.
    expect(computeBackoff(100, 1000, 2.0, 1, midRng)).toBe(150);
  });

  it("caps at maxBackoffMs once scaled exceeds it", () => {
    // 100 * 2^4 = 1600, capped to 1000. Range [100, 1000], midpoint = 550.
    expect(computeBackoff(100, 1000, 2.0, 4, midRng)).toBe(550);
  });

  it("respects RNG bounds", () => {
    expect(computeBackoff(100, 1000, 2.0, 2, minRng)).toBe(100);
    expect(computeBackoff(100, 1000, 2.0, 2, maxRng)).toBe(400);
  });

  it("clamps at maxBackoffMs for huge attempt counts (no Infinity leakage)", () => {
    expect(computeBackoff(100, 5000, 2.0, 10_000, maxRng)).toBe(5000);
  });

  it("skips the RNG when ceiling==initial", () => {
    let calls = 0;
    const countingRng = (lo: number, _hi: number) => {
      calls += 1;
      return lo;
    };
    expect(computeBackoff(100, 1000, 2.0, 0, countingRng)).toBe(100);
    expect(computeBackoff(100, 1000, 1.0, 5, countingRng)).toBe(100);
    expect(calls).toBe(0);
  });

  it("with the default RNG, samples spread continuously across [initial, ceiling]", () => {
    const samples: number[] = [];
    for (let i = 0; i < 1000; i++) {
      samples.push(computeBackoff(100, 1000, 2.0, 4));
    }
    const min = Math.min(...samples);
    const max = Math.max(...samples);
    const mean = samples.reduce((a, b) => a + b, 0) / samples.length;
    expect(min).toBeLessThan(200);
    expect(max).toBeGreaterThan(900);
    expect(mean).toBeGreaterThan(400);
    expect(mean).toBeLessThan(700);
  });
});

describe("replay buffer", () => {
  it("accumulates events with running byte count", () => {
    const t = newTransport(1024);
    t._bufferForReplayForTest(payloadEvent("aaa"), 3);
    t._bufferForReplayForTest(payloadEvent("bbbbb"), 5);
    expect(t._replayBufferSize()).toBe(2);
    expect(t._replayBufferBytes()).toBe(8);
  });

  it("clear drops everything", () => {
    const t = newTransport(1024);
    t._bufferForReplayForTest(payloadEvent("a"), 1);
    t._bufferForReplayForTest(payloadEvent("b"), 1);
    t._clearReplayForTest();
    expect(t._replayBufferSize()).toBe(0);
    expect(t._replayBufferBytes()).toBe(0);
  });

  it("FIFO eviction at the cap", () => {
    const t = newTransport(10);
    t._bufferForReplayForTest(payloadEvent("aaaa"), 4); // total 4
    t._bufferForReplayForTest(payloadEvent("bbbb"), 4); // total 8
    t._bufferForReplayForTest(payloadEvent("cccc"), 4); // total 12 -> evict a, 8
    t._bufferForReplayForTest(payloadEvent("dddd"), 4); // total 12 -> evict b, 8
    expect(t._replayBufferSize()).toBe(2);
    expect(t._replayBufferBytes()).toBe(8);
  });

  it("maxReplayBufferBytes=0 disables buffering", () => {
    const t = newTransport(0);
    t._bufferForReplayForTest(payloadEvent("a"), 1);
    expect(t._replayBufferSize()).toBe(0);
    expect(t._replayBufferBytes()).toBe(0);
  });

  it("oversized single event is dropped immediately by eviction", () => {
    const t = newTransport(10);
    t._bufferForReplayForTest(payloadEvent("0123456789ABCDEF"), 16);
    expect(t._replayBufferSize()).toBe(0);
    expect(t._replayBufferBytes()).toBe(0);
  });
});

describe("payload ack", () => {
  it("real-data payload resets retry counters and clears replay", () => {
    const t = newTransport(1024);
    t._setRetryStateForTest(3, Date.now() - 500, Date.now() + 500);
    t._bufferForReplayForTest(payloadEvent("audio"), 5);
    t._handlePayloadAckForTest('{"channel":"abc"}');
    expect(t._getRetryAttemptForTest()).toBe(0);
    expect(t._replayBufferBytes()).toBe(0);
  });

  it("Metadata does not count as an ack", () => {
    const t = newTransport(1024);
    t._setRetryStateForTest(3, Date.now(), 0);
    t._bufferForReplayForTest(payloadEvent("audio"), 5);
    t._handlePayloadAckForTest('{"type":"Metadata","duration":0}');
    expect(t._getRetryAttemptForTest()).toBe(3);
    expect(t._replayBufferBytes()).toBe(5);
  });

  it("Error does not count as an ack", () => {
    const t = newTransport(1024);
    t._setRetryStateForTest(2, Date.now(), 0);
    t._bufferForReplayForTest(payloadEvent("audio"), 5);
    t._handlePayloadAckForTest('{"type":"Error"}');
    expect(t._getRetryAttemptForTest()).toBe(2);
    expect(t._replayBufferBytes()).toBe(5);
  });

  it("binary payload counts as an ack", () => {
    const t = newTransport(1024);
    t._setRetryStateForTest(1, Date.now(), 0);
    t._bufferForReplayForTest(payloadEvent("audio"), 5);
    t._handlePayloadAckForTest(new Uint8Array([0, 1, 2]));
    expect(t._getRetryAttemptForTest()).toBe(0);
    expect(t._replayBufferBytes()).toBe(0);
  });
});
