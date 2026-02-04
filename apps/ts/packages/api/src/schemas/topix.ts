import { z } from '@hono/zod-openapi';
import { DateSchema } from './common';

/**
 * TOPIX data point schema
 */
export const ApiTopixDataPointSchema = z
  .object({
    date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'Must be YYYY-MM-DD format'),
    open: z.number().positive('Open value must be positive'),
    high: z.number().positive('High value must be positive'),
    low: z.number().positive('Low value must be positive'),
    close: z.number().positive('Close value must be positive'),
    volume: z.number().nonnegative('Volume must be non-negative'),
  })
  .openapi('ApiTopixDataPoint', {
    description: 'TOPIX index data point for a specific date',
  });

/**
 * TOPIX data response schema
 */
export const ApiTopixDataResponseSchema = z
  .object({
    topix: z.array(ApiTopixDataPointSchema),
    lastUpdated: z.string().datetime(),
  })
  .openapi('ApiTopixDataResponse', {
    description: 'Historical TOPIX index data',
  });

/**
 * TOPIX query parameters schema
 */
export const TopixQuerySchema = z
  .object({
    from: DateSchema.optional(),
    to: DateSchema.optional(),
    date: DateSchema.optional(),
  })
  .openapi('TopixQuery', {
    description: 'Optional date range or specific date for TOPIX data. If not specified, returns all available data.',
  });
