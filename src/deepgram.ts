/**
 * Local copy of the JS SDK transport contract.
 *
 * These types intentionally mirror the hand-maintained transport seam added in
 * deepgram-js-sdk PR #492 so this package can compile independently while the
 * SDK work is still landing.
 */
export type DeepgramTransportMessage = string | ArrayBuffer | Blob | ArrayBufferView;

export interface DeepgramTransportCloseEvent {
  code?: number;
  reason?: string;
}

export interface DeepgramTransportRequest {
  url: string;
  headers: Record<string, string>;
  protocols: string[];
  path: string;
  service: "agent.v1" | "listen.v1" | "listen.v2" | "speak.v1";
  queryParams: Record<string, unknown>;
  debug: boolean;
  reconnectAttempts: number;
  connectionTimeoutInSeconds?: number;
  abortSignal?: AbortSignal;
}

export interface DeepgramTransport {
  send(data: DeepgramTransportMessage): void | Promise<void>;
  onOpen(listener: () => void): void;
  onMessage(listener: (message: DeepgramTransportMessage) => void): void;
  onError(listener: (error: Error) => void): void;
  onClose(listener: (event: DeepgramTransportCloseEvent) => void): void;
  isOpen(): boolean;
  close(code?: number, reason?: string): void | Promise<void>;
  ping?(data?: string | ArrayBuffer | Blob | ArrayBufferView): void | Promise<void>;
}

export type DeepgramTransportFactory = (
  url: string,
  headers: Record<string, string>,
  request: DeepgramTransportRequest,
) => DeepgramTransport | Promise<DeepgramTransport>;
