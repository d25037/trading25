import { z } from '@hono/zod-openapi';

/**
 * Daily quotes query schema
 */
export const DailyQuotesQuerySchema = z
  .object({
    code: z.string().openapi({
      example: '7203',
      description: 'Stock code',
    }),
    from: z.string().optional().openapi({
      example: '2024-01-01',
      description: 'Start date (YYYY-MM-DD)',
    }),
    to: z.string().optional().openapi({
      example: '2024-12-31',
      description: 'End date (YYYY-MM-DD)',
    }),
    date: z.string().optional().openapi({
      example: '2024-12-01',
      description: 'Specific date (YYYY-MM-DD)',
    }),
  })
  .openapi('DailyQuotesQuery', {
    description: 'Query parameters for daily quotes',
  });

/**
 * Daily quotes response schema - JQuants API v2 format
 */
export const ApiDailyQuotesResponseSchema = z
  .object({
    data: z.array(
      z.object({
        Date: z.string(),
        Code: z.string(),
        O: z.number().nullable(),
        H: z.number().nullable(),
        L: z.number().nullable(),
        C: z.number().nullable(),
        UL: z.number().nullable(),
        LL: z.number().nullable(),
        Vo: z.number().nullable(),
        Va: z.number().nullable(),
        AdjFactor: z.number(),
        AdjO: z.number().nullable(),
        AdjH: z.number().nullable(),
        AdjL: z.number().nullable(),
        AdjC: z.number().nullable(),
        AdjVo: z.number().nullable(),
      })
    ),
    pagination_key: z.string().optional(),
  })
  .openapi('DailyQuotesResponse', {
    description: 'Raw daily quotes data from JQuants API v2',
  });
