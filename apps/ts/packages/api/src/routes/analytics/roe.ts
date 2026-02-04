import { createRoute } from '@hono/zod-openapi';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import { ROEQuerySchema, ROEResponseSchema } from '../../schemas/roe';
import { ROEDataService } from '../../services/roe-data';
import { createErrorResponse, createOpenAPIApp, handleRouteError } from '../../utils';

const roeApp = createOpenAPIApp();

/**
 * Get ROE analysis route
 */
const getROERoute = createRoute({
  method: 'get',
  path: '/api/analytics/roe',
  tags: ['Analytics'],
  summary: 'Calculate Return on Equity (ROE)',
  description:
    'Fetch financial statements from JQuants API and calculate ROE for specified stocks. Either code or date parameter is required.',
  request: {
    query: ROEQuerySchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: ROEResponseSchema,
        },
      },
      description: 'ROE calculation results with summary statistics',
    },
    400: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Invalid request parameters (either code or date is required)',
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
 * Get ROE analysis handler
 */
roeApp.openapi(getROERoute, async (c) => {
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  // Validate that either code or date is provided
  if (!query.code && !query.date) {
    return c.json(
      createErrorResponse({
        error: 'Bad Request',
        message: 'Either code or date parameter is required',
        details: [
          {
            field: 'code',
            message: 'Provide stock codes (comma-separated)',
          },
          {
            field: 'date',
            message: 'Or provide a date (YYYYMMDD or YYYY-MM-DD)',
          },
        ],
        correlationId,
      }),
      400
    );
  }

  try {
    const roeDataService = new ROEDataService();
    const roeData = await roeDataService.calculateROE({
      code: query.code,
      date: query.date,
      annualize: query.annualize,
      preferConsolidated: query.preferConsolidated,
      minEquity: query.minEquity,
      sortBy: query.sortBy,
      limit: query.limit,
    });

    return c.json(roeData, 200);
  } catch (error) {
    return handleRouteError(c, error, correlationId, {
      operationName: 'calculate ROE',
      logContext: { code: query.code, date: query.date },
      allowedStatusCodes: [400, 500] as const,
    });
  }
});

export default roeApp;
