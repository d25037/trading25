import { normalizeShikihoCode, parseShikihoCaptureTrace, type ShikihoTraceMode } from './contract';
import type { ShikihoExtractionResult } from './extractor';
import type { ProgressiveCaptureRequest, ProgressiveCaptureResult } from './progressive-capture';

const MAX_REQUEST_ID_LENGTH = 256;

export type ShikihoTabRequest =
  | { type: 'probe_shikiho_code' }
  | {
      type: 'capture_now';
      requestId: string;
      attemptId: string;
      code: string;
      mode: ShikihoTraceMode;
      deadlineMs: number;
      receiverAttempts: number;
      receiverReadyMs: number;
      startedAtMs: number;
      probeMs: number;
      acquisitionMs: number;
      receiverMs: number;
    };

export type ProbeShikihoCodeResponse = {
  type: 'shikiho_code';
  code: string | null;
};

export type CaptureNowResponse = {
  type: 'capture_result';
  requestId: string;
  attemptId: string;
  code: string;
  result: ShikihoExtractionResult;
  trace: ProgressiveCaptureResult['trace'];
};

export type RuntimeMessageListener = (
  message: unknown,
  sender: unknown,
  sendResponse: (response?: ProbeShikihoCodeResponse | CaptureNowResponse) => void
) => boolean | undefined;

export interface ShikihoTabBridgeOptions {
  getCode(): string | null;
  capture(request: ProgressiveCaptureRequest): ProgressiveCaptureResult | Promise<ProgressiveCaptureResult>;
  addMessageListener(listener: RuntimeMessageListener): void;
  removeMessageListener(listener: RuntimeMessageListener): void;
}

type InstrumentedCaptureRequest = Extract<ShikihoTabRequest, { attemptId: string }>;

function hasExactKeys(value: Record<string, unknown>, keys: string[]): boolean {
  const actual = Object.keys(value).sort();
  const expected = [...keys].sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
}

const TRACE_MODES = new Set<ShikihoTraceMode>([
  'exact_user_tab',
  'new_owned_tab',
  'warm_owned_same_code',
  'warm_owned_navigated',
]);

function parseRequest(value: unknown): ShikihoTabRequest | null {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return null;
  const request = value as Record<string, unknown>;
  if (request.type === 'probe_shikiho_code' && hasExactKeys(request, ['type'])) {
    return { type: 'probe_shikiho_code' };
  }
  if (
    request.type === 'capture_now' &&
    hasExactKeys(request, [
      'type',
      'requestId',
      'attemptId',
      'code',
      'mode',
      'deadlineMs',
      'receiverAttempts',
      'receiverReadyMs',
      'startedAtMs',
      'probeMs',
      'acquisitionMs',
      'receiverMs',
    ]) &&
    typeof request.requestId === 'string' &&
    request.requestId.length > 0 &&
    request.requestId.length <= MAX_REQUEST_ID_LENGTH &&
    typeof request.attemptId === 'string' &&
    request.attemptId.length > 0 &&
    request.attemptId.length <= MAX_REQUEST_ID_LENGTH &&
    typeof request.code === 'string' &&
    normalizeShikihoCode(request.code) === request.code &&
    typeof request.mode === 'string' &&
    TRACE_MODES.has(request.mode as ShikihoTraceMode) &&
    typeof request.deadlineMs === 'number' &&
    Number.isFinite(request.deadlineMs) &&
    request.deadlineMs >= 0 &&
    typeof request.receiverAttempts === 'number' &&
    Number.isSafeInteger(request.receiverAttempts) &&
    request.receiverAttempts >= 0 &&
    typeof request.receiverReadyMs === 'number' &&
    Number.isFinite(request.receiverReadyMs) &&
    request.receiverReadyMs >= 0 &&
    typeof request.startedAtMs === 'number' &&
    Number.isFinite(request.startedAtMs) &&
    request.startedAtMs >= 0 &&
    typeof request.probeMs === 'number' &&
    Number.isFinite(request.probeMs) &&
    request.probeMs >= 0 &&
    typeof request.acquisitionMs === 'number' &&
    Number.isFinite(request.acquisitionMs) &&
    request.acquisitionMs >= 0 &&
    typeof request.receiverMs === 'number' &&
    Number.isFinite(request.receiverMs) &&
    request.receiverMs >= 0
  ) {
    return request as ShikihoTabRequest;
  }
  return null;
}

async function respondToCapture(
  options: ShikihoTabBridgeOptions,
  request: InstrumentedCaptureRequest,
  sendResponse: (response?: ProbeShikihoCodeResponse | CaptureNowResponse) => void
): Promise<void> {
  try {
    const captureRequest: ProgressiveCaptureRequest = {
      attemptId: request.attemptId,
      code: request.code,
      mode: request.mode,
      deadlineMs: request.deadlineMs,
      receiverAttempts: request.receiverAttempts,
      receiverReadyMs: request.receiverReadyMs,
      startedAtMs: request.startedAtMs,
      probeMs: request.probeMs,
      acquisitionMs: request.acquisitionMs,
      receiverMs: request.receiverMs,
    };
    const { result, trace: rawTrace } = await options.capture(captureRequest);
    if (normalizeShikihoCode(options.getCode()) !== request.code) return;
    const trace = parseShikihoCaptureTrace(rawTrace);
    const resultCode = result.kind === 'success' ? result.snapshot.code : result.code;
    if (
      trace === null ||
      trace.attemptId !== request.attemptId ||
      trace.code !== request.code ||
      resultCode !== request.code
    ) {
      return;
    }
    sendResponse({
      type: 'capture_result',
      requestId: request.requestId,
      attemptId: request.attemptId,
      code: request.code,
      result,
      trace,
    });
  } catch {
    sendResponse(undefined);
  }
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
    void respondToCapture(options, request, sendResponse);
    return true;
  };

  options.addMessageListener(listener);
  return () => options.removeMessageListener(listener);
}
