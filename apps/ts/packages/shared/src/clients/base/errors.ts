export interface JQuantsApiErrorOptions {
  message: string;
  status: number;
  statusText: string;
  responseBody?: unknown;
  isNetworkError?: boolean;
  isTimeoutError?: boolean;
}

const RETRYABLE_STATUS_CODES = new Set([408, 429, 500, 502, 503, 504]);

export class JQuantsApiError extends Error {
  readonly status: number;
  readonly statusText: string;
  readonly responseBody: unknown;
  readonly isNetworkError: boolean;
  readonly isTimeoutError: boolean;

  constructor(options: JQuantsApiErrorOptions) {
    super(options.message);
    this.name = 'JQuantsApiError';
    this.status = options.status;
    this.statusText = options.statusText;
    this.responseBody = options.responseBody;
    this.isNetworkError = options.isNetworkError ?? false;
    this.isTimeoutError = options.isTimeoutError ?? false;
  }

  static isJQuantsApiError(error: unknown): error is JQuantsApiError {
    return error instanceof JQuantsApiError;
  }

  isRetryable(): boolean {
    if (this.isNetworkError || this.isTimeoutError) {
      return true;
    }
    return RETRYABLE_STATUS_CODES.has(this.status);
  }
}
