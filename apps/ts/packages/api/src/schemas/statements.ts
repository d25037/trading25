import { z } from '@hono/zod-openapi';

/**
 * Statements query schema
 */
export const StatementsQuerySchema = z
  .object({
    code: z.string().openapi({
      example: '6857',
      description: 'Stock code (4-5 digits)',
    }),
  })
  .openapi('StatementsQuery', {
    description: 'Query parameters for financial statements',
  });

/**
 * Statements response schema - Raw JQuants API v2 format
 * Only includes forecast EPS-related fields for debugging purposes
 */
export const ApiStatementsResponseSchema = z
  .object({
    data: z.array(
      z.object({
        DiscDate: z.string(),
        Code: z.string(),
        CurPerType: z.string(),
        CurPerSt: z.string(),
        CurPerEn: z.string(),
        // Actual EPS
        EPS: z.number().nullable(),
        // Forecast EPS - Current FY
        FEPS: z.number().nullable(),
        // Forecast EPS - Next FY
        NxFEPS: z.number().nullable(),
        // Non-Consolidated
        NCEPS: z.number().nullable(),
        FNCEPS: z.number().nullable(),
        NxFNCEPS: z.number().nullable(),
      })
    ),
    pagination_key: z.string().optional(),
  })
  .openapi('StatementsResponse', {
    description: 'Raw financial statements data from JQuants API v2 (EPS fields subset)',
  });

/**
 * Raw statements response schema - Complete JQuants API v2 format
 * Includes all fields needed for fundamentals calculation
 */
export const RawStatementsResponseSchema = z
  .object({
    data: z.array(
      z.object({
        // Identification
        DiscDate: z.string(),
        Code: z.string(),
        DocType: z.string(),
        // Period Information
        CurPerType: z.string(),
        CurPerSt: z.string(),
        CurPerEn: z.string(),
        CurFYSt: z.string(),
        CurFYEn: z.string(),
        NxtFYSt: z.string().nullable(),
        NxtFYEn: z.string().nullable(),
        // Financial Performance (Consolidated)
        Sales: z.number().nullable(),
        OP: z.number().nullable(),
        OdP: z.number().nullable(),
        NP: z.number().nullable(),
        EPS: z.number().nullable(),
        DEPS: z.number().nullable(),
        // Financial Position (Consolidated)
        TA: z.number().nullable(),
        Eq: z.number().nullable(),
        EqAR: z.number().nullable(),
        BPS: z.number().nullable(),
        // Cash Flow
        CFO: z.number().nullable(),
        CFI: z.number().nullable(),
        CFF: z.number().nullable(),
        CashEq: z.number().nullable(),
        // Share Information
        ShOutFY: z.number().nullable(),
        TrShFY: z.number().nullable(),
        AvgSh: z.number().nullable(),
        // Forecast EPS
        FEPS: z.number().nullable(),
        NxFEPS: z.number().nullable(),
        // Non-Consolidated Financial Performance
        NCSales: z.number().nullable(),
        NCOP: z.number().nullable(),
        NCOdP: z.number().nullable(),
        NCNP: z.number().nullable(),
        NCEPS: z.number().nullable(),
        // Non-Consolidated Financial Position
        NCTA: z.number().nullable(),
        NCEq: z.number().nullable(),
        NCEqAR: z.number().nullable(),
        NCBPS: z.number().nullable(),
        // Non-Consolidated Forecast EPS
        FNCEPS: z.number().nullable(),
        NxFNCEPS: z.number().nullable(),
      })
    ),
  })
  .openapi('RawStatementsResponse', {
    description: 'Complete raw financial statements data from JQuants API v2 for fundamentals calculation',
  });
