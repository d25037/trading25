/**
 * Safe Parser Utilities
 *
 * Utility functions for safely parsing user input with proper error handling.
 * These functions throw descriptive errors instead of returning NaN or corrupted values.
 */

import { BadRequestError } from '@trading25/shared';

/**
 * Safely parse a string to an integer.
 * Throws BadRequestError if the value is not a valid integer.
 *
 * @param value - The string value to parse
 * @param fieldName - The field name for error messages
 * @returns The parsed integer
 * @throws BadRequestError if the value is not a valid integer
 *
 * @example
 * ```typescript
 * const portfolioId = safeParseInt(c.req.param('id'), 'portfolioId');
 * ```
 */
export function safeParseInt(value: string, fieldName: string): number {
  const parsed = Number.parseInt(value, 10);
  if (Number.isNaN(parsed)) {
    throw new BadRequestError(`Invalid ${fieldName}: must be a valid integer`);
  }
  return parsed;
}

/**
 * Safely decode a URI component.
 * Throws BadRequestError if the value contains malformed URI encoding.
 *
 * @param value - The URI-encoded string to decode
 * @param fieldName - The field name for error messages
 * @returns The decoded string
 * @throws BadRequestError if the value contains malformed URI encoding
 *
 * @example
 * ```typescript
 * const portfolioName = safeDecodeURIComponent(c.req.param('name'), 'portfolioName');
 * ```
 */
export function safeDecodeURIComponent(value: string, fieldName: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    throw new BadRequestError(`Invalid ${fieldName}: malformed URI encoding`);
  }
}
