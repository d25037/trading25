import { z } from '@hono/zod-openapi';

/**
 * Margin long pressure data point schema
 */
export const ApiMarginLongPressureDataSchema = z
  .object({
    date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
    pressure: z.number().describe('(LongVol - ShrtVol) / N-day avg volume'),
    longVol: z.number().nonnegative(),
    shortVol: z.number().nonnegative(),
    avgVolume: z.number().nonnegative(),
  })
  .openapi('ApiMarginLongPressureData', {
    description: 'Margin long pressure data for a specific date',
  });

/**
 * Margin flow pressure data point schema
 */
export const ApiMarginFlowPressureDataSchema = z
  .object({
    date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
    flowPressure: z.number().describe('Delta(LongVol - ShrtVol) / N-day avg volume'),
    currentNetMargin: z.number(),
    previousNetMargin: z.number().nullable(),
    avgVolume: z.number().nonnegative(),
  })
  .openapi('ApiMarginFlowPressureData', {
    description: 'Margin flow pressure data for a specific date',
  });

/**
 * Margin turnover days data point schema
 */
export const ApiMarginTurnoverDaysDataSchema = z
  .object({
    date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
    turnoverDays: z.number().nonnegative().describe('LongVol / N-day avg volume'),
    longVol: z.number().nonnegative(),
    avgVolume: z.number().nonnegative(),
  })
  .openapi('ApiMarginTurnoverDaysData', {
    description: 'Margin turnover days data for a specific date',
  });

/**
 * Margin pressure indicators response schema
 */
export const ApiMarginPressureIndicatorsResponseSchema = z
  .object({
    symbol: z.string().min(4).max(4),
    averagePeriod: z.number().int().positive().describe('Rolling average period in days'),
    longPressure: z.array(ApiMarginLongPressureDataSchema),
    flowPressure: z.array(ApiMarginFlowPressureDataSchema),
    turnoverDays: z.array(ApiMarginTurnoverDaysDataSchema),
    lastUpdated: z.string().datetime(),
  })
  .openapi('ApiMarginPressureIndicatorsResponse', {
    description: 'Margin pressure indicators with configurable period',
  });

/**
 * Margin pressure query parameters schema
 */
export const MarginPressureQuerySchema = z
  .object({
    period: z.coerce
      .number()
      .int()
      .min(5, 'Period must be at least 5 days')
      .max(60, 'Period must be at most 60 days')
      .default(15)
      .openapi({
        param: {
          name: 'period',
          in: 'query',
        },
        example: 15,
        description: 'Rolling average period in days (default: 15)',
      }),
  })
  .openapi('MarginPressureQuery');
