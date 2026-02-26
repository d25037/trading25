import { HttpRequestError, requestJson } from '@trading25/api-clients/base/http-client';

export type QueryPrimitive = string | number | boolean | undefined;

export function toQueryString(params: Record<string, QueryPrimitive>): string {
  const queryParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined) {
      queryParams.append(key, String(value));
    }
  }
  return queryParams.toString();
}

export class BaseApiClient {
  protected readonly baseUrl: string;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl;
  }

  protected async request<T>(endpoint: string, options?: RequestInit): Promise<T> {
    try {
      return await requestJson<T>(endpoint, {
        baseUrl: this.baseUrl,
        ...options,
        headers: {
          'Content-Type': 'application/json',
          ...options?.headers,
        },
      });
    } catch (error) {
      this.handleRequestError(error);
    }
  }

  private handleRequestError(error: unknown): never {
    if (error instanceof HttpRequestError && error.kind === 'http') {
      const body = error.body as { message?: unknown } | undefined;
      if (body && typeof body.message === 'string' && body.message.trim().length > 0) {
        throw new Error(body.message);
      }
      throw new Error(`HTTP error! status: ${error.status ?? 'unknown'}`);
    }

    const message = error instanceof HttpRequestError ? error.message : error instanceof Error ? error.message : '';
    if (message.includes('fetch failed') || message.includes('ECONNREFUSED')) {
      throw new Error(
        'Cannot connect to API server. Please ensure bt FastAPI is running with "uv run bt server --port 3002"'
      );
    }

    if (message) {
      throw new Error(message);
    }
    throw new Error('Unknown error occurred');
  }
}
