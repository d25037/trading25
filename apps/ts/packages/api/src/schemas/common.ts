import { z } from '@hono/zod-openapi';

/**
 * Unified error response schema
 * All error responses follow this structure with correlationId always included
 */
export const ErrorResponseSchema = z
  .object({
    status: z.literal('error'),
    error: z.enum([
      'Bad Request',
      'Not Found',
      'Conflict',
      'Unprocessable Entity',
      'Not Implemented',
      'Internal Server Error',
    ]),
    message: z.string(),
    details: z
      .array(
        z.object({
          field: z.string(),
          message: z.string(),
        })
      )
      .optional(),
    timestamp: z.string().datetime(),
    correlationId: z.string().uuid(),
  })
  .openapi('ErrorResponse', {
    description: 'Standard error response with correlation tracking',
  });

/**
 * Timeframe parameter schema
 */
export const TimeframeSchema = z.enum(['daily', 'weekly', 'monthly']).default('daily').openapi({
  description: 'Data aggregation timeframe',
  example: 'daily',
});

/**
 * Adjusted price parameter schema
 */
export const AdjustedSchema = z
  .enum(['true', 'false'])
  .default('true')
  .transform((val) => val !== 'false')
  .openapi({
    description: 'Whether to return adjusted prices (default: true)',
    example: 'true',
  });

/**
 * Date parameter schema (YYYY-MM-DD format)
 */
export const DateSchema = z
  .string()
  .regex(/^\d{4}-\d{2}-\d{2}$/, 'Must be YYYY-MM-DD format')
  .openapi({
    description: 'Date in YYYY-MM-DD format',
    example: '2024-01-01',
  });
