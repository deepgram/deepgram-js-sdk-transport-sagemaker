import { describe, expect, it, vi } from "vitest";

import { createSageMakerTransportFactory } from "../src/factory";
import { SageMakerTransportFactory as SageMakerTransportFactoryAlias } from "../src/factory";
import type { DeepgramTransportRequest } from "../src/deepgram";
import { SageMakerTransport } from "../src/transport";
import type { SageMakerRuntimeHTTP2ClientLike } from "../src/types";

function makeRequest(overrides: Partial<DeepgramTransportRequest> = {}): DeepgramTransportRequest {
  return {
    url: "wss://api.deepgram.com/v1/listen?model=nova-3&language=en",
    headers: {},
    protocols: [],
    path: "/v1/listen",
    service: "listen.v1",
    queryParams: {},
    debug: false,
    reconnectAttempts: 0,
    ...overrides,
  };
}

describe("createSageMakerTransportFactory", () => {
  it("creates transports from Deepgram websocket request metadata", () => {
    const client: SageMakerRuntimeHTTP2ClientLike = {
      send: vi.fn(),
      destroy: vi.fn(),
    };

    const factory = createSageMakerTransportFactory(
      {
        endpointName: "my-endpoint",
        region: "us-east-1",
      },
      client,
    );

    const transport = factory("wss://api.deepgram.com/v1/listen?model=nova-3&language=en", {}, makeRequest());

    expect(transport).toBeInstanceOf(SageMakerTransport);
    expect(factory.config.endpointName).toBe("my-endpoint");
    expect(factory.config.region).toBe("us-east-1");
    expect(factory.client).toBe(client);
  });

  it("defaults region to us-west-2", () => {
    const factory = createSageMakerTransportFactory(
      {
        endpointName: "my-endpoint",
      },
      {
        send: vi.fn(),
      },
    );

    expect(factory.config.region).toBe("us-west-2");
  });

  it("rejects blank endpoint names", () => {
    expect(() =>
      createSageMakerTransportFactory(
        {
          endpointName: "   ",
        },
        {
          send: vi.fn(),
        },
      ),
    ).toThrow("endpointName is required");
  });

  it("exposes destroy on the returned transport factory", () => {
    const client: SageMakerRuntimeHTTP2ClientLike = {
      send: vi.fn(),
      destroy: vi.fn(),
    };

    const factory = createSageMakerTransportFactory({ endpointName: "my-endpoint" }, client);
    factory.destroy();

    expect(client.destroy).toHaveBeenCalledOnce();
  });

  it("falls back to the websocket URL path when request.path is empty", () => {
    const factory = createSageMakerTransportFactory(
      {
        endpointName: "my-endpoint",
      },
      {
        send: vi.fn(),
      },
    );

    const transport = factory(
      "wss://api.deepgram.com/v2/listen?model=flux-general-en",
      {},
      makeRequest({
        path: "",
        service: "listen.v2",
      }),
    );

    expect(transport).toBeInstanceOf(SageMakerTransport);
  });

  it("exports a naming-parity alias for the factory creator", () => {
    expect(SageMakerTransportFactoryAlias).toBe(createSageMakerTransportFactory);
  });
});
