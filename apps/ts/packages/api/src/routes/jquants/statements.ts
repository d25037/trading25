import { createRoute } from '@hono/zod-openapi';
import { logger } from '@trading25/shared/utils/logger';
import { ErrorResponseSchema } from '../../schemas/common';
import {
  ApiStatementsResponseSchema,
  RawStatementsResponseSchema,
  StatementsQuerySchema,
} from '../../schemas/statements';
import { BaseJQuantsService } from '../../services/base-jquants-service';
import { createErrorResponse, createOpenAPIApp } from '../../utils';

class StatementsProxyService extends BaseJQuantsService {
  async getStatements(code: string) {
    const client = this.getJQuantsClient();
    return this.withTokenRefresh(() => client.getStatements({ code }));
  }
}

const statementsService = new StatementsProxyService();

const statementsApp = createOpenAPIApp();

/**
 * Get financial statements route
 *
 * 🔧 Layer 1: JQuants Proxy API
 *
 * Purpose: Returns raw JQuants financial statements for debugging forecast EPS fields
 */
const getStatementsRoute = createRoute({
  method: 'get',
  path: '/api/jquants/statements',
  tags: ['JQuants Proxy'],
  summary: '🔧 Get financial statements (raw JQuants format)',
  description: `**Layer 1: JQuants Proxy API** - Raw financial statements for debugging

**Purpose**: Returns EPS-related fields from JQuants statements API for verifying forecast EPS logic.

**Key Fields**:
- \`EPS\`: Actual earnings per share
- \`FEPS\`: Current FY forecast EPS (for FY statements = completed year, for Q statements = in-progress year)
- \`NxFEPS\`: Next FY forecast EPS

⚠️ **For production applications**, use \`/api/analytics/fundamentals/{symbol}\` instead.`,
  request: {
    query: StatementsQuerySchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: ApiStatementsResponseSchema,
        },
      },
      description: 'Financial statements retrieved successfully',
    },
    400: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Invalid request parameters',
    },
    500: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Internal server error',
    },
  },
});

statementsApp.openapi(getStatementsRoute, async (c) => {
  const { code } = c.req.valid('query');
  const correlationId = c.get('correlationId') || c.req.header('x-correlation-id') || crypto.randomUUID();

  try {
    const response = await statementsService.getStatements(code);

    const data = response.data.map((stmt) => ({
      DiscDate: stmt.DiscDate,
      Code: stmt.Code,
      CurPerType: stmt.CurPerType,
      CurPerSt: stmt.CurPerSt,
      CurPerEn: stmt.CurPerEn,
      EPS: stmt.EPS,
      FEPS: stmt.FEPS,
      NxFEPS: stmt.NxFEPS,
      NCEPS: stmt.NCEPS,
      FNCEPS: stmt.FNCEPS,
      NxFNCEPS: stmt.NxFNCEPS,
    }));

    return c.json({ data }, 200);
  } catch (error) {
    logger.error('Failed to fetch statements', {
      correlationId,
      params: { code },
      error: error instanceof Error ? error.message : String(error),
    });
    return c.json(
      createErrorResponse({
        error: 'Internal Server Error',
        message: error instanceof Error ? error.message : 'Failed to fetch statements',
        correlationId,
      }),
      500
    );
  }
});

/**
 * Get raw financial statements route (complete data)
 *
 * 🔧 Layer 1: JQuants Proxy API
 *
 * Purpose: Returns complete raw JQuants financial statements for apps/bt/ fundamentals calculation
 */
const getRawStatementsRoute = createRoute({
  method: 'get',
  path: '/api/jquants/statements/raw',
  tags: ['JQuants Proxy'],
  summary: '🔧 Get raw financial statements (complete JQuants format)',
  description: `**Layer 1: JQuants Proxy API** - Complete raw financial statements for fundamentals calculation

**Purpose**: Returns all financial fields from JQuants statements API for apps/bt/ fundamentals service.

**Key Fields**:
- Financial Performance: Sales, OP, OdP, NP, EPS, DEPS
- Financial Position: TA, Eq, EqAR, BPS
- Cash Flow: CFO, CFI, CFF, CashEq
- Share Information: ShOutFY, TrShFY, AvgSh
- Forecast EPS: FEPS, NxFEPS, FNCEPS, NxFNCEPS
- Non-Consolidated: NCSales, NCOP, NCNP, NCEPS, NCBPS, etc.

🔗 **Used by**: apps/bt/ fundamentals service (\`POST /bt/api/fundamentals/compute\`)`,
  request: {
    query: StatementsQuerySchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: RawStatementsResponseSchema,
        },
      },
      description: 'Complete raw financial statements retrieved successfully',
    },
    400: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Invalid request parameters',
    },
    500: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Internal server error',
    },
  },
});

statementsApp.openapi(getRawStatementsRoute, async (c) => {
  const { code } = c.req.valid('query');
  const correlationId = c.get('correlationId') || c.req.header('x-correlation-id') || crypto.randomUUID();

  try {
    const response = await statementsService.getStatements(code);

    const data = response.data.map((stmt) => ({
      // Identification
      DiscDate: stmt.DiscDate,
      Code: stmt.Code,
      DocType: stmt.DocType,
      // Period Information
      CurPerType: stmt.CurPerType,
      CurPerSt: stmt.CurPerSt,
      CurPerEn: stmt.CurPerEn,
      CurFYSt: stmt.CurFYSt,
      CurFYEn: stmt.CurFYEn,
      NxtFYSt: stmt.NxtFYSt,
      NxtFYEn: stmt.NxtFYEn,
      // Financial Performance (Consolidated)
      Sales: stmt.Sales,
      OP: stmt.OP,
      OdP: stmt.OdP,
      NP: stmt.NP,
      EPS: stmt.EPS,
      DEPS: stmt.DEPS,
      // Financial Position (Consolidated)
      TA: stmt.TA,
      Eq: stmt.Eq,
      EqAR: stmt.EqAR,
      BPS: stmt.BPS,
      // Cash Flow
      CFO: stmt.CFO,
      CFI: stmt.CFI,
      CFF: stmt.CFF,
      CashEq: stmt.CashEq,
      // Share Information
      ShOutFY: stmt.ShOutFY,
      TrShFY: stmt.TrShFY,
      AvgSh: stmt.AvgSh,
      // Forecast EPS
      FEPS: stmt.FEPS,
      NxFEPS: stmt.NxFEPS,
      // Non-Consolidated Financial Performance
      NCSales: stmt.NCSales,
      NCOP: stmt.NCOP,
      NCOdP: stmt.NCOdP,
      NCNP: stmt.NCNP,
      NCEPS: stmt.NCEPS,
      // Non-Consolidated Financial Position
      NCTA: stmt.NCTA,
      NCEq: stmt.NCEq,
      NCEqAR: stmt.NCEqAR,
      NCBPS: stmt.NCBPS,
      // Non-Consolidated Forecast EPS
      FNCEPS: stmt.FNCEPS,
      NxFNCEPS: stmt.NxFNCEPS,
    }));

    return c.json({ data }, 200);
  } catch (error) {
    logger.error('Failed to fetch raw statements', {
      correlationId,
      params: { code },
      error: error instanceof Error ? error.message : String(error),
    });
    return c.json(
      createErrorResponse({
        error: 'Internal Server Error',
        message: error instanceof Error ? error.message : 'Failed to fetch raw statements',
        correlationId,
      }),
      500
    );
  }
});

export default statementsApp;
