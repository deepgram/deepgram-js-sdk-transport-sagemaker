import { SageMakerRuntimeHTTP2Client } from "@aws-sdk/client-sagemaker-runtime-http2";

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
  const runtimeClient =
    client ??
    new SageMakerRuntimeHTTP2Client({
      ...resolvedConfig.clientConfig,
      region: resolvedConfig.region,
    });

  const create = (url: string, _headers: Record<string, string>, request: DeepgramTransportRequest) => {
    return new SageMakerTransport({
      client: runtimeClient,
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
      value: runtimeClient,
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
        runtimeClient.destroy?.();
      },
    },
  });

  return transportFactory;
}

export const SageMakerTransportFactory = createSageMakerTransportFactory;
