/**
 * API Client utilities for consistent error handling
 */

import {
  buildQueryString as buildQueryStringBase,
  buildUrl as buildUrlBase,
  extractErrorMessage as extractHttpErrorMessage,
  HttpRequestError,
  requestJson,
} from '@trading25/clients-ts/base/http-client';

/**
 * Custom API error class with status code and details
 */
export class ApiError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    public readonly details?: unknown
  ) {
    super(message);
    this.name = 'ApiError';
  }

  /**
   * Check if error is a client error (4xx)
   */
  isClientError(): boolean {
    return this.status >= 400 && this.status < 500;
  }

  /**
   * Check if error is a server error (5xx)
   */
  isServerError(): boolean {
    return this.status >= 500;
  }
}

/**
 * Query parameters type - supports strings, numbers, booleans, and undefined
 */
export type QueryParams = Record<string, string | number | boolean | undefined>;

/**
 * Build a query string from parameters, filtering out undefined values
 */
export function buildQueryString(params: QueryParams): string {
  return buildQueryStringBase(params);
}

/**
 * Build a full URL with optional query parameters
 */
export function buildUrl(path: string, params?: QueryParams): string {
  return buildUrlBase(path, params);
}

function toApiError(error: HttpRequestError): ApiError {
  const status = error.status ?? 500;
  const defaultMessage = `Request failed: ${error.statusText || 'Unknown Error'}`;
  const message = extractHttpErrorMessage(error.body) || defaultMessage;
  return new ApiError(message, status, error.body);
}

/**
 * Perform a GET request with error handling
 */
export async function apiGet<T>(path: string, params?: QueryParams): Promise<T> {
  try {
    return await requestJson<T>(buildUrl(path, params));
  } catch (error) {
    if (error instanceof HttpRequestError && error.kind === 'http') {
      throw toApiError(error);
    }
    throw error;
  }
}

/**
 * Perform a POST request with error handling
 */
export async function apiPost<T>(path: string, data?: unknown): Promise<T> {
  try {
    return await requestJson<T>(path, {
      method: 'POST',
      headers: data ? { 'Content-Type': 'application/json' } : undefined,
      body: data ? JSON.stringify(data) : undefined,
    });
  } catch (error) {
    if (error instanceof HttpRequestError && error.kind === 'http') {
      throw toApiError(error);
    }
    throw error;
  }
}

/**
 * Perform a PUT request with error handling
 */
export async function apiPut<T>(path: string, data: unknown): Promise<T> {
  try {
    return await requestJson<T>(path, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });
  } catch (error) {
    if (error instanceof HttpRequestError && error.kind === 'http') {
      throw toApiError(error);
    }
    throw error;
  }
}

/**
 * Perform a DELETE request with error handling
 */
export async function apiDelete<T>(path: string): Promise<T> {
  try {
    return await requestJson<T>(path, { method: 'DELETE' });
  } catch (error) {
    if (error instanceof HttpRequestError && error.kind === 'http') {
      throw toApiError(error);
    }
    throw error;
  }
}
