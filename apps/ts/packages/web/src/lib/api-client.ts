/**
 * API Client utilities for consistent error handling
 */

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
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined) {
      searchParams.set(key, String(value));
    }
  }
  return searchParams.toString();
}

/**
 * Build a full URL with optional query parameters
 */
export function buildUrl(path: string, params?: QueryParams): string {
  if (!params) return path;
  const queryString = buildQueryString(params);
  return queryString ? `${path}?${queryString}` : path;
}

/**
 * Extract error message from response body
 */
async function extractErrorMessage(response: Response, defaultMessage: string): Promise<string> {
  try {
    const body = await response.json();
    return body.message || defaultMessage;
  } catch {
    return defaultMessage;
  }
}

/**
 * Perform a GET request with error handling
 */
export async function apiGet<T>(path: string, params?: QueryParams): Promise<T> {
  const url = buildUrl(path, params);
  const response = await fetch(url);
  if (!response.ok) {
    const message = await extractErrorMessage(response, `Request failed: ${response.statusText}`);
    throw new ApiError(message, response.status);
  }
  return response.json();
}

/**
 * Perform a POST request with error handling
 */
export async function apiPost<T>(path: string, data?: unknown): Promise<T> {
  const response = await fetch(path, {
    method: 'POST',
    headers: data ? { 'Content-Type': 'application/json' } : undefined,
    body: data ? JSON.stringify(data) : undefined,
  });
  if (!response.ok) {
    const message = await extractErrorMessage(response, `Request failed: ${response.statusText}`);
    throw new ApiError(message, response.status);
  }
  return response.json();
}

/**
 * Perform a PUT request with error handling
 */
export async function apiPut<T>(path: string, data: unknown): Promise<T> {
  const response = await fetch(path, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  });
  if (!response.ok) {
    const message = await extractErrorMessage(response, `Request failed: ${response.statusText}`);
    throw new ApiError(message, response.status);
  }
  return response.json();
}

/**
 * Perform a DELETE request with error handling
 */
export async function apiDelete<T>(path: string): Promise<T> {
  const response = await fetch(path, { method: 'DELETE' });
  if (!response.ok) {
    const message = await extractErrorMessage(response, `Request failed: ${response.statusText}`);
    throw new ApiError(message, response.status);
  }
  return response.json();
}
