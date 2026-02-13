export type QueryParamValue = string | number | boolean | null | undefined;
export type QueryParams = Record<string, QueryParamValue>;

export type HttpRequestErrorKind = 'http' | 'network' | 'timeout' | 'invalid-json';

interface HttpRequestErrorOptions {
  status?: number;
  statusText?: string;
  body?: unknown;
  cause?: unknown;
}

export class HttpRequestError extends Error {
  readonly kind: HttpRequestErrorKind;
  readonly status?: number;
  readonly statusText?: string;
  readonly body?: unknown;

  constructor(message: string, kind: HttpRequestErrorKind, options: HttpRequestErrorOptions = {}) {
    super(message, options.cause !== undefined ? { cause: options.cause } : undefined);
    this.name = 'HttpRequestError';
    this.kind = kind;
    this.status = options.status;
    this.statusText = options.statusText;
    this.body = options.body;
  }
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
  const rawResponse = response.clone();
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

  const message = extractErrorMessage(errorBody) || response.statusText || `HTTP ${response.status}`;
  throw new HttpRequestError(message, 'http', {
    status: response.status,
    statusText: response.statusText,
    body: errorBody,
  });
}

async function parseJsonResponse<T>(response: Response): Promise<T> {
  try {
    return (await response.json()) as T;
  } catch (cause) {
    throw new HttpRequestError('Invalid JSON response', 'invalid-json', {
      status: response.status,
      statusText: response.statusText,
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
