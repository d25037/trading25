import { createRoute } from '@hono/zod-openapi';
import { logger } from '@trading25/shared/utils/logger';
import { ErrorResponseSchema } from '../../schemas/common';
import { ApiDailyQuotesResponseSchema, DailyQuotesQuerySchema } from '../../schemas/daily-quotes';
import { DailyQuotesDataService } from '../../services/daily-quotes-data';
import { createErrorResponse, createOpenAPIApp } from '../../utils';

const dailyQuotesDataService = new DailyQuotesDataService();

const dailyQuotesApp = createOpenAPIApp();

/**
 * Get daily quotes route
 *
 * 🔧 Layer 1: JQuants Proxy API
 *
 * Purpose: Returns raw JQuants API data for debugging and verification
 *
 * Use Cases:
 * - Debug JQuants API responses
 * - Verify data integrity
 * - Compare raw vs processed data
 * - Custom data processing development
 *
 * ⚠️ For production applications, use /api/chart/stocks/{symbol} instead
 */
const getDailyQuotesRoute = createRoute({
  method: 'get',
  path: '/api/jquants/daily-quotes',
  tags: ['JQuants Proxy'],
  summary: '🔧 Get daily stock quotes (raw JQuants format)',
  description: `**Layer 1: JQuants Proxy API** - Raw data for debugging and development

**Purpose**: Returns unprocessed JQuants API data with all original fields preserved.

**Use Cases**:
- Debugging JQuants API responses
- Data integrity verification
- Custom data processing development
- Comparing raw vs processed data

⚠️ **For production applications**, use \`/api/chart/stocks/{symbol}\` instead (optimized, cached, chart-ready format).

**Characteristics**:
- Raw JQuants field names (e.g., "Date", "Code", "Open", "High", "Low", "Close")
- No caching applied
- Minimal processing overhead
- Complete data structure from JQuants`,
  request: {
    query: DailyQuotesQuerySchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: ApiDailyQuotesResponseSchema,
        },
      },
      description: 'Daily quotes retrieved successfully',
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

/**
 * Get daily quotes handler
 */
dailyQuotesApp.openapi(getDailyQuotesRoute, async (c) => {
  const { code, from, to, date } = c.req.valid('query');
  const correlationId = c.get('correlationId') || c.req.header('x-correlation-id') || crypto.randomUUID();

  try {
    const params: { code: string; from?: string; to?: string; date?: string } = { code };
    if (from) params.from = from;
    if (to) params.to = to;
    if (date) params.date = date;

    const jquantsResponse = await dailyQuotesDataService.getDailyQuotes(params);

    return c.json(jquantsResponse, 200);
  } catch (error) {
    logger.error('Failed to fetch daily quotes', {
      correlationId,
      params: { code, from, to, date },
      error: error instanceof Error ? error.message : String(error),
    });
    return c.json(
      createErrorResponse({
        error: 'Internal Server Error',
        message: error instanceof Error ? error.message : 'Failed to fetch daily quotes',
        correlationId,
      }),
      500
    );
  }
});

export default dailyQuotesApp;
