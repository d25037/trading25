import { createRoute } from '@hono/zod-openapi';
import { logger } from '@trading25/shared/utils/logger';
import { ErrorResponseSchema } from '../../schemas/common';
import { ApiListedInfoResponseSchema, ListedInfoQuerySchema } from '../../schemas/listed-info';
import { ListedInfoDataService } from '../../services/listed-info-data';
import { createErrorResponse } from '../../utils/error-responses';
import { createOpenAPIApp } from '../../utils/validation-hook';

const listedInfoDataService = new ListedInfoDataService();

const listedInfoApp = createOpenAPIApp();

/**
 * Get listed info route
 */
const getListedInfoRoute = createRoute({
  method: 'get',
  path: '/api/jquants/listed-info',
  tags: ['JQuants Proxy'],
  summary: 'Get listed stock information',
  description: 'Fetch listed stock information with optional filtering by code or date',
  request: {
    query: ListedInfoQuerySchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: ApiListedInfoResponseSchema,
        },
      },
      description: 'Listed info retrieved successfully',
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
 * Get listed info handler
 */
listedInfoApp.openapi(getListedInfoRoute, async (c) => {
  const { code, date } = c.req.valid('query');
  const correlationId = c.get('correlationId') || c.req.header('x-correlation-id') || crypto.randomUUID();

  try {
    const params = code || date ? { code, date } : undefined;
    const jquantsResponse = await listedInfoDataService.getListedInfo(params);

    // Transform JQuants response to API response format
    const apiResponse = {
      info: jquantsResponse.data.map((item) => ({
        code: item.Code,
        companyName: item.CoName,
        companyNameEnglish: item.CoNameEn,
        marketCode: item.Mkt,
        marketCodeName: item.MktNm,
        sector33Code: item.S33,
        sector33CodeName: item.S33Nm,
        scaleCategory: item.ScaleCat,
      })),
      lastUpdated: new Date().toISOString(),
    };

    return c.json(apiResponse, 200);
  } catch (error) {
    logger.error('Failed to fetch listed info', {
      correlationId,
      params: { code, date },
      error: error instanceof Error ? error.message : String(error),
    });
    return c.json(
      createErrorResponse({
        error: 'Internal Server Error',
        message: error instanceof Error ? error.message : 'Failed to fetch listed info',
        correlationId,
      }),
      500
    );
  }
});

export default listedInfoApp;
