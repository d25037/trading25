export type QueryParamValue = string | number | boolean | null | undefined;
export type QueryParams = Record<string, QueryParamValue>;

export type HttpRequestErrorKind = 'http' | 'network' | 'timeout' | 'invalid-json';

interface HttpRequestErrorOptions {
  status?: number;
  statusText?: string;
  body?: unknown;
  details?: unknown[];
  correlationId?: string;
  reason?: string;
  recovery?: string;
  cause?: unknown;
}

export class HttpRequestError extends Error {
  readonly kind: HttpRequestErrorKind;
  readonly status?: number;
  readonly statusText?: string;
  readonly body?: unknown;
  readonly details?: unknown[];
  readonly correlationId?: string;
  readonly reason?: string;
  readonly recovery?: string;

  constructor(message: string, kind: HttpRequestErrorKind, options: HttpRequestErrorOptions = {}) {
    super(message, options.cause !== undefined ? { cause: options.cause } : undefined);
    this.name = 'HttpRequestError';
    this.kind = kind;
    this.status = options.status;
    this.statusText = options.statusText;
    this.body = options.body;
    this.details = options.details;
    this.correlationId = options.correlationId;
    this.reason = options.reason;
    this.recovery = options.recovery;
  }
}

interface UnifiedErrorFields {
  message: string;
  details?: unknown[];
  correlationId?: string;
  reason?: string;
  recovery?: string;
}

function getErrorDetailValue(details: unknown[], fieldName: 'reason' | 'recovery'): string | undefined {
  for (const detail of details) {
    if (
      detail !== null &&
      typeof detail === 'object' &&
      (detail as Record<string, unknown>).field === fieldName &&
      typeof (detail as Record<string, unknown>).message === 'string'
    ) {
      return (detail as Record<string, unknown>).message as string;
    }
  }
  return undefined;
}

function parseUnifiedErrorFields(body: unknown): UnifiedErrorFields | undefined {
  if (body === null || typeof body !== 'object' || Array.isArray(body)) {
    return undefined;
  }

  const record = body as Record<string, unknown>;
  if (typeof record.message !== 'string') {
    return undefined;
  }
  if (record.details !== undefined && !Array.isArray(record.details)) {
    return undefined;
  }
  if (record.correlationId !== undefined && typeof record.correlationId !== 'string') {
    return undefined;
  }

  const details = record.details as unknown[] | undefined;
  return {
    message: record.message,
    details,
    correlationId: record.correlationId as string | undefined,
    reason: details ? getErrorDetailValue(details, 'reason') : undefined,
    recovery: details ? getErrorDetailValue(details, 'recovery') : undefined,
  };
}

export interface JsonRequestOptions extends RequestInit {
  baseUrl?: string;
  query?: QueryParams;
  timeoutMs?: number;
}

function isAbsoluteUrl(path: string): boolean {
  return /^[a-zA-Z][a-zA-Z\d+\-.]*:/.test(path);
}

export function buildQueryString(params: QueryParams): string {
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null) {
      searchParams.set(key, String(value));
    }
  }
  return searchParams.toString();
}

export function buildUrl(path: string, params?: QueryParams, baseUrl?: string): string {
  const query = params ? buildQueryString(params) : '';

  if (baseUrl || isAbsoluteUrl(path)) {
    const url = baseUrl ? new URL(path, baseUrl) : new URL(path);
    if (query) {
      const queryParams = new URLSearchParams(query);
      for (const [key, value] of queryParams) {
        url.searchParams.set(key, value);
      }
    }
    return url.toString();
  }

  if (!query) {
    return path;
  }

  const delimiter = path.includes('?') ? '&' : '?';
  return `${path}${delimiter}${query}`;
}

export function extractErrorMessage(body: unknown): string | undefined {
  if (body && typeof body === 'object') {
    const maybeMessage = (body as Record<string, unknown>).message;
    if (typeof maybeMessage === 'string' && maybeMessage.trim().length > 0) {
      return maybeMessage;
    }

    const maybeError = (body as Record<string, unknown>).error;
    if (typeof maybeError === 'string' && maybeError.trim().length > 0) {
      return maybeError;
    }

    const maybeDetail = (body as Record<string, unknown>).detail;
    if (typeof maybeDetail === 'string' && maybeDetail.trim().length > 0) {
      return maybeDetail;
    }
  }

  if (typeof body === 'string' && body.trim().length > 0) {
    return body;
  }

  return undefined;
}

interface TimeoutSignalResult {
  signal?: AbortSignal;
  cleanup: () => void;
  didTimeout: () => boolean;
}

function reportHttpClientObservation(message: string, context: Record<string, unknown>): void {
  if (process.env.NODE_ENV === 'test') {
    return;
  }
  console.warn(`[http-client] ${message}`, context);
}

function createTimeoutSignal(inputSignal: AbortSignal | null | undefined, timeoutMs?: number): TimeoutSignalResult {
  if (!timeoutMs || timeoutMs <= 0) {
    return { signal: inputSignal ?? undefined, cleanup: () => {}, didTimeout: () => false };
  }

  const controller = new AbortController();
  let timedOut = false;
  const timeoutId = setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, timeoutMs);

  const onAbort = () => controller.abort();
  if (inputSignal) {
    if (inputSignal.aborted) {
      controller.abort();
    } else {
      inputSignal.addEventListener('abort', onAbort, { once: true });
    }
  }

  return {
    signal: controller.signal,
    cleanup: () => {
      clearTimeout(timeoutId);
      if (inputSignal) {
        inputSignal.removeEventListener('abort', onAbort);
      }
    },
    didTimeout: () => timedOut,
  };
}

async function throwHttpError(response: Response): Promise<never> {
  let errorBody: unknown;
  const rawResponse = typeof response.clone === 'function' ? response.clone() : response;
  try {
    errorBody = await response.json();
  } catch (parseError) {
    try {
      const textBody = await rawResponse.text();
      errorBody = textBody.trim().length > 0 ? textBody : undefined;
    } catch (textError) {
      reportHttpClientObservation('Failed to read non-JSON error response body', {
        status: response.status,
        statusText: response.statusText,
        parseError: parseError instanceof Error ? parseError.message : String(parseError),
        textError: textError instanceof Error ? textError.message : String(textError),
      });
    }

    reportHttpClientObservation('Received non-JSON error response body', {
      status: response.status,
      statusText: response.statusText,
      parseError: parseError instanceof Error ? parseError.message : String(parseError),
    });
  }

  const unifiedError = parseUnifiedErrorFields(errorBody);
  const message = unifiedError?.message || extractErrorMessage(errorBody) || response.statusText || `HTTP ${response.status}`;
  throw new HttpRequestError(message, 'http', {
    status: response.status,
    statusText: response.statusText,
    body: errorBody,
    details: unifiedError?.details,
    correlationId: unifiedError?.correlationId,
    reason: unifiedError?.reason,
    recovery: unifiedError?.recovery,
  });
}

async function parseJsonResponse<T>(response: Response): Promise<T> {
  let text: string;
  try {
    text = await response.text();
  } catch (cause) {
    throw new HttpRequestError('Invalid JSON response', 'invalid-json', {
      status: response.status,
      statusText: response.statusText,
      cause,
    });
  }

  if (text.length === 0) {
    throw new HttpRequestError('Empty response body', 'invalid-json', {
      status: response.status,
      statusText: response.statusText,
    });
  }

  try {
    return JSON.parse(text) as T;
  } catch (cause) {
    const parseMessage = cause instanceof Error ? cause.message : String(cause);
    const parseReason = parseMessage.slice(0, 80);
    const bodyPreview = text.slice(0, 150);
    throw new HttpRequestError(`Invalid JSON response (${parseReason}): ${bodyPreview}`, 'invalid-json', {
      status: response.status,
      statusText: response.statusText,
      body: text,
      cause,
    });
  }
}

function toTransportError(error: unknown, timeout: TimeoutSignalResult, timeoutMs?: number): HttpRequestError {
  if (error instanceof HttpRequestError) {
    return error;
  }

  if (error instanceof DOMException && error.name === 'AbortError' && timeout.didTimeout()) {
    return new HttpRequestError(`Request timed out after ${timeoutMs}ms`, 'timeout', { cause: error });
  }

  const message = error instanceof Error ? error.message : 'Network request failed';
  return new HttpRequestError(message, 'network', { cause: error });
}

export async function requestJson<T>(path: string, options: JsonRequestOptions = {}): Promise<T> {
  const { baseUrl, query, timeoutMs, ...requestInit } = options;
  const url = buildUrl(path, query, baseUrl);
  const timeout = createTimeoutSignal(requestInit.signal, timeoutMs);
  const init = timeout.signal ? { ...requestInit, signal: timeout.signal } : requestInit;
  const hasInit = Object.keys(init).length > 0;

  try {
    const response = hasInit ? await fetch(url, init) : await fetch(url);

    if (!response.ok) {
      await throwHttpError(response);
    }

    return await parseJsonResponse<T>(response);
  } catch (error) {
    throw toTransportError(error, timeout, timeoutMs);
  } finally {
    timeout.cleanup();
  }
}
