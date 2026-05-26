import { SageMakerRuntimeHTTP2Client } from "@aws-sdk/client-sagemaker-runtime-http2";
import { NodeHttp2Handler } from "@smithy/node-http-handler";

import { resolveSageMakerConfig } from "./config";
import type { DeepgramTransportRequest } from "./deepgram";
import { SageMakerTransport } from "./transport";
import type {
  SageMakerConfig,
  SageMakerRuntimeHTTP2ClientLike,
  SageMakerTransportFactory as SageMakerTransportFactoryFn,
} from "./types";

function getInvocationPath(url: string, request: DeepgramTransportRequest): string {
  const requestPath = request.path.trim();

  if (requestPath.length > 0) {
    return requestPath.replace(/^\/+/, "");
  }

  const invocationPath = new URL(url).pathname.replace(/^\/+/, "");

  if (!invocationPath) {
    throw new Error(`Could not determine SageMaker invocation path from URL: ${url}`);
  }

  return invocationPath;
}

function getQueryString(url: string): string {
  const search = new URL(url).search;
  return search.startsWith("?") ? search.slice(1) : search;
}

export function createSageMakerTransportFactory(
  config: SageMakerConfig,
  client?: SageMakerRuntimeHTTP2ClientLike,
): SageMakerTransportFactoryFn {
  const resolvedConfig = resolveSageMakerConfig(config);

  // Construct one shared AWS HTTP/2 client per factory. The SageMaker JS
  // SDK's default request handler sets `disableConcurrentStreams: true`,
  // which forces 1 stream per TCP connection -- at 400 concurrent streams
  // from one process, that means 400 fresh TLS handshakes, and AWS rejects
  // a substantial fraction with UnknownError under that load.
  //
  // Override the request handler with our own NodeHttp2Handler configured
  // for stream multiplexing (`disableConcurrentStreams: false`), matching
  // Python's smithy stack default. With multiplexing, all 400 streams
  // share an HTTP/2 connection pool sized per server-advertised
  // SETTINGS_MAX_CONCURRENT_STREAMS (typically 100), so we open ~4-5 TCP
  // connections instead of 400 -- and AWS stops rejecting.
  const sharedClient: SageMakerRuntimeHTTP2ClientLike =
    client ??
    new SageMakerRuntimeHTTP2Client({
      ...resolvedConfig.clientConfig,
      region: resolvedConfig.region,
      requestHandler: new NodeHttp2Handler({
        disableConcurrentStreams: false,
      }),
    });

  const create = (url: string, _headers: Record<string, string>, request: DeepgramTransportRequest) => {
    return new SageMakerTransport({
      client: sharedClient,
      config: resolvedConfig,
      invocationPath: getInvocationPath(url, request),
      queryString: getQueryString(url),
      service: request.service,
      abortSignal: request.abortSignal,
    });
  };

  const transportFactory = ((url: string, headers: Record<string, string>, request: DeepgramTransportRequest) => {
    return create(url, headers, request);
  }) as unknown as SageMakerTransportFactoryFn;

  Object.defineProperties(transportFactory, {
    client: {
      value: sharedClient,
      enumerable: true,
    },
    config: {
      value: Object.freeze({ ...resolvedConfig }),
      enumerable: true,
    },
    create: {
      value: create,
    },
    destroy: {
      value: () => {
        try {
          sharedClient.destroy?.();
        } catch {
          // best-effort
        }
      },
    },
  });

  return transportFactory;
}

export const SageMakerTransportFactory = createSageMakerTransportFactory;
