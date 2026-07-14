import { normalizeShikihoCode } from './contract';
import type { ShikihoExtractionResult } from './extractor';

const MAX_REQUEST_ID_LENGTH = 256;

export type ShikihoTabRequest =
  | { type: 'probe_shikiho_code' }
  | { type: 'capture_now'; requestId: string; code: string; waitForReady: boolean };

export type ProbeShikihoCodeResponse = {
  type: 'shikiho_code';
  code: string | null;
};

export type CaptureNowResponse = {
  type: 'capture_result';
  requestId: string;
  code: string;
  result: ShikihoExtractionResult;
};

export type RuntimeMessageListener = (
  message: unknown,
  sender: unknown,
  sendResponse: (response?: ProbeShikihoCodeResponse | CaptureNowResponse) => void
) => boolean | undefined;

export interface ShikihoTabBridgeOptions {
  getCode(): string | null;
  capture(): ShikihoExtractionResult | Promise<ShikihoExtractionResult>;
  waitUntilReady(): Promise<void>;
  addMessageListener(listener: RuntimeMessageListener): void;
  removeMessageListener(listener: RuntimeMessageListener): void;
}

function hasExactKeys(value: Record<string, unknown>, keys: string[]): boolean {
  const actual = Object.keys(value).sort();
  const expected = [...keys].sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
}

function parseRequest(value: unknown): ShikihoTabRequest | null {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return null;
  const request = value as Record<string, unknown>;
  if (request.type === 'probe_shikiho_code' && hasExactKeys(request, ['type'])) {
    return { type: 'probe_shikiho_code' };
  }
  if (
    request.type === 'capture_now' &&
    hasExactKeys(request, ['type', 'requestId', 'code', 'waitForReady']) &&
    typeof request.requestId === 'string' &&
    request.requestId.length > 0 &&
    request.requestId.length <= MAX_REQUEST_ID_LENGTH &&
    typeof request.code === 'string' &&
    normalizeShikihoCode(request.code) === request.code &&
    typeof request.waitForReady === 'boolean'
  ) {
    return request as ShikihoTabRequest;
  }
  return null;
}

export function startShikihoTabBridge(options: ShikihoTabBridgeOptions): () => void {
  const listener: RuntimeMessageListener = (rawMessage, _sender, sendResponse) => {
    const request = parseRequest(rawMessage);
    if (request === null) return;
    if (request.type === 'probe_shikiho_code') {
      sendResponse({ type: 'shikiho_code', code: normalizeShikihoCode(options.getCode()) });
      return;
    }

    if (normalizeShikihoCode(options.getCode()) !== request.code) return;
    void (async () => {
      try {
        if (request.waitForReady) await options.waitUntilReady();
        const result = await options.capture();
        if (normalizeShikihoCode(options.getCode()) !== request.code) {
          sendResponse(undefined);
          return;
        }
        sendResponse({
          type: 'capture_result',
          requestId: request.requestId,
          code: request.code,
          result,
        });
      } catch {
        sendResponse(undefined);
      }
    })();
    return true;
  };

  options.addMessageListener(listener);
  return () => options.removeMessageListener(listener);
}
