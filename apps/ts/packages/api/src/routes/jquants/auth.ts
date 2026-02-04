import { createRoute } from '@hono/zod-openapi';
import { JQuantsClient } from '@trading25/shared';
import { logger } from '@trading25/shared/utils/logger';
import { AuthStatusResponseSchema } from '../../schemas/auth';
import { ErrorResponseSchema } from '../../schemas/common';
import { createErrorResponse, createOpenAPIApp } from '../../utils';

const authApp = createOpenAPIApp();

/**
 * Auth status route - JQuants API v2
 * Checks if API key is configured
 */
const authStatusRoute = createRoute({
  method: 'get',
  path: '/api/jquants/auth/status',
  tags: ['JQuants Proxy'],
  summary: 'Get JQuants API v2 authentication status',
  description: 'Check if JQUANTS_API_KEY is configured for API v2 authentication',
  responses: {
    200: {
      content: {
        'application/json': {
          schema: AuthStatusResponseSchema,
        },
      },
      description: 'Authentication status retrieved successfully',
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
 * Auth status handler
 */
authApp.openapi(authStatusRoute, async (c) => {
  const correlationId = c.get('correlationId') || c.req.header('x-correlation-id') || crypto.randomUUID();

  try {
    logger.debug('Checking JQuants API v2 auth status', { correlationId });

    // Check if API key is configured
    const apiKey = process.env.JQUANTS_API_KEY;
    const hasApiKey = !!apiKey;

    // Create client to validate (optional)
    if (hasApiKey) {
      new JQuantsClient({ apiKey });
    }

    return c.json(
      {
        authenticated: hasApiKey,
        hasApiKey,
      },
      200
    );
  } catch (error) {
    logger.error('Failed to check auth status', {
      correlationId,
      error: error instanceof Error ? error.message : String(error),
    });

    return c.json(
      createErrorResponse({
        error: 'Internal Server Error',
        message: error instanceof Error ? error.message : 'Failed to check auth status',
        correlationId,
      }),
      500
    );
  }
});

export default authApp;
