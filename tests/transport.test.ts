import type {
  InvokeEndpointWithBidirectionalStreamCommand,
  InvokeEndpointWithBidirectionalStreamCommandOutput,
  ResponseStreamEvent,
} from "@aws-sdk/client-sagemaker-runtime-http2";
import { describe, expect, it } from "vitest";

import { resolveSageMakerConfig } from "../src/config";
import { AsyncQueue } from "../src/queue";
import { SageMakerTransport } from "../src/transport";
import type { SageMakerRuntimeHTTP2ClientLike } from "../src/types";

const config = resolveSageMakerConfig({
  endpointName: "test-endpoint",
  region: "us-west-2",
});

function flush(): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, 0);
  });
}

class MockClient implements SageMakerRuntimeHTTP2ClientLike {
  public readonly commands: InvokeEndpointWithBidirectionalStreamCommand[] = [];
  public readonly responses = new AsyncQueue<ResponseStreamEvent>();
  public readonly options: Array<{ abortSignal?: AbortSignal } | undefined> = [];

  public async send(
    command: InvokeEndpointWithBidirectionalStreamCommand,
    options?: { abortSignal?: AbortSignal },
  ): Promise<InvokeEndpointWithBidirectionalStreamCommandOutput> {
    this.commands.push(command);
    this.options.push(options);
    return {
      Body: this.responses,
      $metadata: {},
    };
  }
}

class EagerReaderClient implements SageMakerRuntimeHTTP2ClientLike {
  public firstEvent: Promise<IteratorResult<any>> | undefined;

  public async send(
    command: InvokeEndpointWithBidirectionalStreamCommand,
  ): Promise<InvokeEndpointWithBidirectionalStreamCommandOutput> {
    this.firstEvent = command.input.Body![Symbol.asyncIterator]().next();

    return {
      Body: new AsyncQueue<ResponseStreamEvent>(),
      $metadata: {},
    };
  }
}

describe("SageMakerTransport", () => {
  it("starts open and fires onOpen immediately", async () => {
    const transport = new SageMakerTransport({
      client: new MockClient(),
      config,
      invocationPath: "v1/listen",
      queryString: "model=nova-3",
      service: "listen.v1",
    });

    let opened = false;
    transport.onOpen(() => {
      opened = true;
    });

    await flush();

    expect(transport.isOpen()).toBe(true);
    expect(opened).toBe(true);
  });

  it("creates a SageMaker bidi stream on first send", async () => {
    const client = new MockClient();
    const transport = new SageMakerTransport({
      client,
      config: {
        ...config,
        targetVariant: "primary",
      },
      invocationPath: "v1/listen",
      queryString: "model=nova-3&interim_results=true",
      service: "listen.v1",
    });

    await transport.send('{"type":"KeepAlive"}');

    expect(client.commands).toHaveLength(1);
    expect(client.commands[0]?.input.EndpointName).toBe("test-endpoint");
    expect(client.commands[0]?.input.ModelInvocationPath).toBe("v1/listen");
    expect(client.commands[0]?.input.ModelQueryString).toBe("model=nova-3&interim_results=true");
    expect(client.commands[0]?.input.TargetVariant).toBe("primary");

    const iterator = client.commands[0]!.input.Body![Symbol.asyncIterator]();
    const firstEvent = await iterator.next();

    expect(firstEvent.done).toBe(false);
    expect(firstEvent.value).toEqual({
      PayloadPart: {
        Bytes: new TextEncoder().encode('{"type":"KeepAlive"}'),
        DataType: "UTF8",
      },
    });
  });

  it("queues the first payload before the client starts reading the request stream", async () => {
    const client = new EagerReaderClient();
    const transport = new SageMakerTransport({
      client,
      config,
      invocationPath: "v1/listen",
      queryString: "model=nova-3",
      service: "listen.v1",
    });

    await transport.send('{"type":"KeepAlive"}');

    await expect(client.firstEvent).resolves.toEqual({
      done: false,
      value: {
        PayloadPart: {
          Bytes: new TextEncoder().encode('{"type":"KeepAlive"}'),
          DataType: "UTF8",
        },
      },
    });
  });

  it("sends binary media as BINARY payload parts", async () => {
    const client = new MockClient();
    const transport = new SageMakerTransport({
      client,
      config,
      invocationPath: "v1/listen",
      queryString: "model=nova-3",
      service: "listen.v1",
    });

    await transport.send(new Uint8Array([1, 2, 3]));

    const iterator = client.commands[0]!.input.Body![Symbol.asyncIterator]();
    const firstEvent = await iterator.next();

    expect(firstEvent.value).toEqual({
      PayloadPart: {
        Bytes: new Uint8Array([1, 2, 3]),
        DataType: "BINARY",
      },
    });
  });

  it("forwards UTF8 and binary response messages", async () => {
    const client = new MockClient();
    const transport = new SageMakerTransport({
      client,
      config,
      invocationPath: "v1/speak",
      queryString: "model=aura-asteria-en",
      service: "speak.v1",
    });

    const messages: Array<string | Uint8Array> = [];
    transport.onMessage((message) => {
      messages.push(message as string | Uint8Array);
    });

    await transport.send('{"type":"Speak"}');

    client.responses.push({
      PayloadPart: {
        Bytes: new TextEncoder().encode('{"type":"Metadata"}'),
        DataType: "UTF8",
      },
    });
    client.responses.push({
      PayloadPart: {
        Bytes: new Uint8Array([9, 8, 7]),
        DataType: "BINARY",
      },
    });

    await flush();

    expect(messages[0]).toBe('{"type":"Metadata"}');
    expect(messages[1]).toEqual(new Uint8Array([9, 8, 7]));
  });

  it("surfaces a ModelStreamError when retries are disabled", async () => {
    const client = new MockClient();
    const transport = new SageMakerTransport({
      client,
      config: { ...config, maxRetries: 0 },
      invocationPath: "v1/listen",
      queryString: "model=nova-3",
      service: "listen.v1",
    });

    const errors: Error[] = [];
    transport.onError((error) => {
      errors.push(error);
    });

    await transport.send(new Uint8Array([1]));

    client.responses.push({
      ModelStreamError: {
        Message: "model failed",
        ErrorCode: "ModelError",
      },
    } as ResponseStreamEvent);

    await flush();

    expect(errors).toHaveLength(1);
    expect(errors[0]?.message).toBe("model failed");
    expect(transport.isOpen()).toBe(false);
  });

  it("forwards abort signals to the SageMaker client", async () => {
    const client = new MockClient();
    const abortController = new AbortController();
    const transport = new SageMakerTransport({
      client,
      config,
      invocationPath: "v1/listen",
      queryString: "model=nova-3",
      service: "listen.v1",
      abortSignal: abortController.signal,
    });

    await transport.send(new Uint8Array([1]));

    // The transport composes the caller's abortSignal with an internal
    // subscription-timeout signal, so the signal passed to the client isn't
    // the exact original. Verify the composition by aborting the caller's
    // signal and confirming the composed signal aborts too.
    const composed = client.options[0]?.abortSignal;
    expect(composed).toBeDefined();
    expect(composed?.aborted).toBe(false);
    abortController.abort();
    expect(composed?.aborted).toBe(true);
  });

  it("implements ping as a Deepgram keepalive message for listening services", async () => {
    const client = new MockClient();
    const transport = new SageMakerTransport({
      client,
      config,
      invocationPath: "v2/listen",
      queryString: "model=flux-general-en",
      service: "listen.v2",
    });

    transport.ping();
    await flush();

    const iterator = client.commands[0]!.input.Body![Symbol.asyncIterator]();
    const firstEvent = await iterator.next();

    expect(firstEvent.value).toEqual({
      PayloadPart: {
        Bytes: new TextEncoder().encode('{"type":"KeepAlive"}'),
        DataType: "UTF8",
      },
    });
  });

  it("emits close when the response stream ends", async () => {
    const client = new MockClient();
    const transport = new SageMakerTransport({
      client,
      config,
      invocationPath: "v1/listen",
      queryString: "model=nova-3",
      service: "listen.v1",
    });

    const closeEvents: Array<{ code?: number; reason?: string }> = [];
    transport.onClose((event) => {
      closeEvents.push(event);
    });

    await transport.send(new Uint8Array([1]));
    client.responses.close();

    await flush();

    expect(closeEvents).toEqual([{ code: 1000, reason: "Normal" }]);
    expect(transport.isOpen()).toBe(false);
  });

  it("closes immediately when its abort signal fires", async () => {
    const abortController = new AbortController();
    const transport = new SageMakerTransport({
      client: new MockClient(),
      config,
      invocationPath: "v1/listen",
      queryString: "model=nova-3",
      service: "listen.v1",
      abortSignal: abortController.signal,
    });

    const closeEvents: Array<{ code?: number; reason?: string }> = [];
    transport.onClose((event) => {
      closeEvents.push(event);
    });

    abortController.abort();
    await flush();

    expect(closeEvents).toEqual([{ code: 1000, reason: "aborted" }]);
    expect(transport.isOpen()).toBe(false);
  });
});
