import { logger } from '@trading25/shared/utils/logger';

const DEFAULT_BT_API_URL = 'http://localhost:3002';
const DEFAULT_TIMEOUT_MS = 30_000;

export class BtApiProxyError extends Error {
  readonly statusCode?: number;

  constructor(message: string, statusCode?: number) {
    super(message);
    this.name = 'BtApiProxyError';
    this.statusCode = statusCode;
  }
}

function toQueryString(query?: Record<string, unknown>): string {
  if (!query) return '';

  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value === undefined || value === null) continue;

    if (Array.isArray(value)) {
      for (const item of value) {
        params.append(key, String(item));
      }
      continue;
    }

    params.append(key, String(value));
  }

  const queryString = params.toString();
  return queryString ? `?${queryString}` : '';
}

function resolveErrorMessage(bodyText: string, status: number): string {
  if (!bodyText) {
    return `apps/bt API request failed with status ${status}`;
  }

  try {
    const body = JSON.parse(bodyText) as {
      message?: string;
      detail?: string;
      error?: string;
    };

    if (typeof body.message === 'string' && body.message.length > 0) {
      return body.message;
    }
    if (typeof body.detail === 'string' && body.detail.length > 0) {
      return body.detail;
    }
    if (typeof body.error === 'string' && body.error.length > 0) {
      return body.error;
    }
  } catch {
    // Response body is not JSON; fall back to raw text.
  }

  return bodyText;
}

function resolveTimeoutMs(rawTimeout: string | undefined): number {
  const parsed = Number(rawTimeout ?? DEFAULT_TIMEOUT_MS);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return DEFAULT_TIMEOUT_MS;
  }
  return parsed;
}

function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export async function btGet<T>(path: string, query?: Record<string, unknown>): Promise<T> {
  const baseUrl = process.env.BT_API_URL ?? DEFAULT_BT_API_URL;
  const timeoutMs = resolveTimeoutMs(process.env.BT_API_TIMEOUT);
  const url = `${baseUrl}${path}${toQueryString(query)}`;

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
      signal: controller.signal,
    });

    const bodyText = await response.text();

    if (!response.ok) {
      const message = resolveErrorMessage(bodyText, response.status);
      throw new BtApiProxyError(message, response.status);
    }

    if (!bodyText) {
      throw new BtApiProxyError('Empty response body from apps/bt API', 500);
    }

    try {
      return JSON.parse(bodyText) as T;
    } catch {
      throw new BtApiProxyError('Invalid JSON response from apps/bt API', 500);
    }
  } catch (error) {
    if (controller.signal.aborted) {
      throw new BtApiProxyError(`apps/bt API request timed out after ${timeoutMs}ms`, 500);
    }

    if (error instanceof BtApiProxyError) {
      throw error;
    }

    logger.warn('apps/bt API proxy request failed', {
      path,
      query,
      error: toErrorMessage(error),
    });

    throw new BtApiProxyError(toErrorMessage(error), 500);
  } finally {
    clearTimeout(timer);
  }
}
