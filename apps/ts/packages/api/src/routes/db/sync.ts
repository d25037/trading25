import { createRoute, z } from '@hono/zod-openapi';
import { logger } from '@trading25/shared/utils/logger';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import {
  CancelJobResponseSchema,
  CreateSyncJobResponseSchema,
  MarketSyncRequestSchema,
  SyncJobResponseSchema,
} from '../../schemas/market-sync';
import { marketSyncService } from '../../services/market/market-sync-service';
import { createErrorResponse, createOpenAPIApp } from '../../utils';

const marketSyncApp = createOpenAPIApp();

/**
 * POST /api/db/sync - Start sync job
 */
const startSyncRoute = createRoute({
  method: 'post',
  path: '/api/db/sync',
  tags: ['Database'],
  summary: 'Start market data synchronization',
  description:
    'Start a new market data sync job. Only one sync job can run at a time. Returns immediately with job ID for polling.',
  request: {
    body: {
      content: {
        'application/json': {
          schema: MarketSyncRequestSchema,
        },
      },
    },
  },
  responses: {
    202: {
      content: {
        'application/json': {
          schema: CreateSyncJobResponseSchema,
        },
      },
      description: 'Sync job started',
    },
    400: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Invalid request parameters',
    },
    409: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Another sync job is already running',
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

marketSyncApp.openapi(startSyncRoute, async (c) => {
  const body = c.req.valid('json');
  const correlationId = getCorrelationId(c);

  logger.info('Sync job requested', { correlationId, mode: body.mode });

  try {
    const result = marketSyncService.startSync(body.mode);

    if (!result) {
      return c.json(
        createErrorResponse({
          error: 'Conflict',
          message: 'Another sync job is already running. Please wait for it to complete or cancel it.',
          correlationId,
        }),
        409
      );
    }

    return c.json(result, 202);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    logger.error('Failed to start sync job', { correlationId, error: errorMessage });
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

/**
 * GET /api/db/sync/jobs/:jobId - Get job status
 */
const getJobStatusRoute = createRoute({
  method: 'get',
  path: '/api/db/sync/jobs/{jobId}',
  tags: ['Database'],
  summary: 'Get sync job status',
  description: 'Get the current status and progress of a sync job.',
  request: {
    params: z.object({
      jobId: z.string().uuid().openapi({ description: 'Job ID', example: '123e4567-e89b-12d3-a456-426614174000' }),
    }),
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: SyncJobResponseSchema,
        },
      },
      description: 'Job status retrieved',
    },
    400: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Invalid job ID format',
    },
    404: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Job not found',
    },
  },
});

marketSyncApp.openapi(getJobStatusRoute, async (c) => {
  const { jobId } = c.req.valid('param');
  const correlationId = getCorrelationId(c);

  const job = marketSyncService.getJobStatus(jobId);

  if (!job) {
    return c.json(
      createErrorResponse({
        error: 'Not Found',
        message: `Job with ID ${jobId} not found`,
        correlationId,
      }),
      404
    );
  }

  return c.json(job, 200);
});

/**
 * DELETE /api/db/sync/jobs/:jobId - Cancel job
 */
const cancelJobRoute = createRoute({
  method: 'delete',
  path: '/api/db/sync/jobs/{jobId}',
  tags: ['Database'],
  summary: 'Cancel sync job',
  description: 'Cancel a running or pending sync job.',
  request: {
    params: z.object({
      jobId: z.string().uuid().openapi({ description: 'Job ID', example: '123e4567-e89b-12d3-a456-426614174000' }),
    }),
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: CancelJobResponseSchema,
        },
      },
      description: 'Job cancelled',
    },
    400: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Invalid job ID or job cannot be cancelled',
    },
    404: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Job not found',
    },
  },
});

marketSyncApp.openapi(cancelJobRoute, async (c) => {
  const { jobId } = c.req.valid('param');
  const correlationId = getCorrelationId(c);

  logger.info('Job cancellation requested', { correlationId, jobId });

  const result = marketSyncService.cancelJob(jobId);

  if (!result.success) {
    // Determine if it's a 404 or 400
    const job = marketSyncService.getJobStatus(jobId);
    if (!job) {
      return c.json(
        createErrorResponse({
          error: 'Not Found',
          message: `Job with ID ${jobId} not found`,
          correlationId,
        }),
        404
      );
    }

    return c.json(
      createErrorResponse({
        error: 'Bad Request',
        message: result.message,
        correlationId,
      }),
      400
    );
  }

  return c.json(
    {
      success: true,
      jobId,
      message: result.message,
    },
    200
  );
});

export default marketSyncApp;
