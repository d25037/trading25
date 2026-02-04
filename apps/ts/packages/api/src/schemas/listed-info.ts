import { z } from '@hono/zod-openapi';
import { DateSchema } from './common';

/**
 * Listed info data point schema
 */
export const ApiListedInfoSchema = z
  .object({
    code: z.string().min(4).max(4),
    companyName: z.string(),
    companyNameEnglish: z.string().optional(),
    marketCode: z.string().optional(),
    marketCodeName: z.string().optional(),
    sector33Code: z.string().optional(),
    sector33CodeName: z.string().optional(),
    scaleCategory: z.string().optional(),
  })
  .openapi('ApiListedInfo', {
    description: 'Listed stock information',
  });

/**
 * Listed info response schema
 */
export const ApiListedInfoResponseSchema = z
  .object({
    info: z.array(ApiListedInfoSchema),
    lastUpdated: z.string().datetime(),
  })
  .openapi('ApiListedInfoResponse', {
    description: 'Listed stock information data',
  });

/**
 * Listed info query parameters schema
 */
export const ListedInfoQuerySchema = z
  .object({
    code: z
      .string()
      .length(4, 'Stock code must be exactly 4 characters')
      .regex(/^[0-9A-Z]+$/, 'Stock code must contain only numbers and uppercase letters')
      .optional()
      .openapi({
        param: {
          name: 'code',
          in: 'query',
        },
        example: '7203',
        description: 'Stock code (4 characters, e.g., 7203 for Toyota)',
      }),
    date: DateSchema.optional(),
  })
  .openapi('ListedInfoQuery', {
    description: 'Optional query parameters for listed info. If code is not specified, returns all stocks.',
  });
