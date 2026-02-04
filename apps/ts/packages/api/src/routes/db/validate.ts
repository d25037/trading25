import { createRoute } from '@hono/zod-openapi';
import { logger } from '@trading25/shared/utils/logger';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import { MarketValidationResponseSchema } from '../../schemas/market-validation';
import { MarketValidationService } from '../../services/market/market-validation-service';
import { createErrorResponse, createManagedService, createOpenAPIApp, detectDatabaseError } from '../../utils';

const getMarketValidationService = createManagedService('MarketValidationService', {
  factory: () => new MarketValidationService(),
});

const marketValidateApp = createOpenAPIApp();

/**
 * Get database validation route
 */
const getMarketValidateRoute = createRoute({
  method: 'get',
  path: '/api/db/validate',
  tags: ['Database'],
  summary: 'Validate market database',
  description:
    'Validate market database integrity, check for missing data, stock split/merger events, and provide recommendations for data maintenance.',
  responses: {
    200: {
      content: {
        'application/json': {
          schema: MarketValidationResponseSchema,
        },
      },
      description: 'Market database validation report',
    },
    422: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Database not found or cannot be opened',
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
 * Get market validation handler
 */
marketValidateApp.openapi(getMarketValidateRoute, async (c) => {
  const correlationId = getCorrelationId(c);
  const marketValidationService = getMarketValidationService();

  try {
    const validationData = await marketValidationService.validate();
    return c.json(validationData, 200);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';

    // Check for database-related errors
    const dbError = detectDatabaseError(errorMessage);
    if (dbError.isDatabaseError) {
      logger.warn('Market database not found or corrupted', {
        correlationId,
        error: errorMessage,
      });
      return c.json(
        createErrorResponse({
          error: 'Unprocessable Entity',
          message: 'Market database not found. Please run "bun cli market sync --init" first.',
          correlationId,
        }),
        422
      );
    }

    logger.error('Failed to validate market database', {
      correlationId,
      error: errorMessage,
    });
    return c.json(
      createErrorResponse({
        error: 'Internal Server Error',
        message: errorMessage,
        correlationId,
      }),
      500
    );
  }
});

export default marketValidateApp;
