import { z } from '@hono/zod-openapi';
import { DateSchema } from './common';

/**
 * Margin interest data point schema
 */
export const ApiMarginInterestSchema = z
  .object({
    date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'Must be YYYY-MM-DD format'),
    code: z.string().min(4).max(4),
    shortMarginTradeVolume: z.number().nonnegative(),
    longMarginTradeVolume: z.number().nonnegative(),
    shortMarginOutstandingBalance: z.number().nonnegative().optional(),
    longMarginOutstandingBalance: z.number().nonnegative().optional(),
  })
  .openapi('ApiMarginInterest', {
    description: 'Weekly margin interest data point',
  });

/**
 * Margin interest response schema
 */
export const ApiMarginInterestResponseSchema = z
  .object({
    marginInterest: z.array(ApiMarginInterestSchema),
    symbol: z.string().min(4).max(4),
    lastUpdated: z.string().datetime(),
  })
  .openapi('ApiMarginInterestResponse', {
    description: 'Weekly margin interest data',
  });

/**
 * Margin interest query parameters schema
 */
export const MarginInterestQuerySchema = z
  .object({
    from: DateSchema.optional(),
    to: DateSchema.optional(),
    date: DateSchema.optional(),
  })
  .openapi('MarginInterestQuery', {
    description: 'Optional date range or specific date for margin interest data.',
  });
