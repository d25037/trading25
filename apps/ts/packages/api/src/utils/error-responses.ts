import type { z } from '@hono/zod-openapi';
import type { TypedResponse } from 'hono';
import type { ErrorResponseSchema } from '../schemas/common';

export type ErrorResponse = z.infer<typeof ErrorResponseSchema>;

export const ERROR_STATUS_CODES = [400, 404, 409, 422, 500] as const;
export type ErrorStatusCode = (typeof ERROR_STATUS_CODES)[number];

export type ErrorResponseResult<Code extends ErrorStatusCode = ErrorStatusCode> = Response &
  TypedResponse<ErrorResponse, Code, 'json'>;

export type ErrorType =
  | 'Bad Request'
  | 'Not Found'
  | 'Conflict'
  | 'Unprocessable Entity'
  | 'Not Implemented'
  | 'Internal Server Error';

export interface ErrorResponseParams {
  error: ErrorType;
  message: string;
  details?: Array<{ field: string; message: string }>;
  correlationId: string;
}

/**
 * Create a standardized error response
 * Ensures all errors include correlationId and follow unified format
 */
export function createErrorResponse(params: ErrorResponseParams): ErrorResponse {
  return {
    status: 'error',
    error: params.error,
    message: params.message,
    details: params.details,
    timestamp: new Date().toISOString(),
    correlationId: params.correlationId,
  };
}

export function isErrorStatusCode(statusCode: number): statusCode is ErrorStatusCode {
  return (ERROR_STATUS_CODES as readonly number[]).includes(statusCode);
}

export function resolveAllowedStatus<Code extends ErrorStatusCode>(
  statusCode: ErrorStatusCode,
  allowedStatusCodes?: readonly Code[]
): Code {
  if (!allowedStatusCodes || allowedStatusCodes.length === 0) {
    return statusCode as Code;
  }

  if (allowedStatusCodes.includes(statusCode as Code)) {
    return statusCode as Code;
  }

  if (allowedStatusCodes.includes(500 as Code)) {
    return 500 as Code;
  }

  return (allowedStatusCodes[0] ?? 500) as Code;
}
