import { z } from '@hono/zod-openapi';
import { DateSchema } from './common';

/**
 * Index data point schema
 */
export const ApiIndexSchema = z
  .object({
    date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'Must be YYYY-MM-DD format'),
    code: z.string().optional(),
    open: z.number().positive('Open value must be positive'),
    high: z.number().positive('High value must be positive'),
    low: z.number().positive('Low value must be positive'),
    close: z.number().positive('Close value must be positive'),
  })
  .openapi('ApiIndex', {
    description: 'Index data point for a specific date',
  });

/**
 * Indices response schema
 */
export const ApiIndicesResponseSchema = z
  .object({
    indices: z.array(ApiIndexSchema),
    lastUpdated: z.string().datetime(),
  })
  .openapi('ApiIndicesResponse', {
    description: 'Historical index data',
  });

/**
 * Indices query parameters schema
 */
export const IndicesQuerySchema = z
  .object({
    code: z
      .string()
      .optional()
      .openapi({
        param: {
          name: 'code',
          in: 'query',
        },
        example: '0000',
        description: 'Index code (e.g., 0000 for Nikkei 225)',
      }),
    from: DateSchema.optional(),
    to: DateSchema.optional(),
    date: DateSchema.optional(),
  })
  .openapi('IndicesQuery', {
    description: 'Optional query parameters for indices data. If not specified, returns recent data.',
  });
