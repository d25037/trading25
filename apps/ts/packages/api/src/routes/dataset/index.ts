import { createRoute, z } from '@hono/zod-openapi';
import { logger } from '@trading25/shared/utils/logger';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import {
  CancelDatasetJobResponseSchema,
  CreateDatasetJobResponseSchema,
  DatasetCreateRequestSchema,
  DatasetDeleteResponseSchema,
  DatasetInfoResponseSchema,
  DatasetJobResponseSchema,
  DatasetListResponseSchema,
  DatasetSampleQuerySchema,
  DatasetSampleResponseSchema,
  DatasetSearchQuerySchema,
  DatasetSearchResponseSchema,
} from '../../schemas/dataset';
import { datasetService } from '../../services/dataset/dataset-service';
import { createErrorResponse, createOpenAPIApp } from '../../utils';

const datasetApp = createOpenAPIApp();

const listDatasetsRoute = createRoute({
  method: 'get',
  path: '/api/dataset',
  tags: ['Dataset'],
  summary: 'List all datasets',
  description: 'Scan the datasets directory and return metadata for each .db file.',
  responses: {
    200: {
      content: {
        'application/json': {
          schema: DatasetListResponseSchema,
        },
      },
      description: 'Dataset list retrieved',
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

datasetApp.openapi(listDatasetsRoute, (c) => {
  const correlationId = getCorrelationId(c);

  try {
    const result = datasetService.listDatasets();
    return c.json(result, 200);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    logger.error('Failed to list datasets', { correlationId, error: errorMessage });
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

const deleteDatasetRoute = createRoute({
  method: 'delete',
  path: '/api/dataset/{name}',
  tags: ['Dataset'],
  summary: 'Delete a dataset',
  description: 'Delete a dataset file from the datasets directory.',
  request: {
    params: z.object({
      name: z.string().min(1).openapi({ description: 'Dataset name', example: 'prime.db' }),
    }),
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: DatasetDeleteResponseSchema,
        },
      },
      description: 'Dataset deleted',
    },
    404: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Dataset not found',
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

datasetApp.openapi(deleteDatasetRoute, (c) => {
  const { name } = c.req.valid('param');
  const correlationId = getCorrelationId(c);

  logger.info('Dataset deletion requested', { correlationId, name });

  try {
    const result = datasetService.deleteDataset(name);

    if (!result.success) {
      return c.json(
        createErrorResponse({
          error: 'Not Found',
          message: result.message,
          correlationId,
        }),
        404
      );
    }

    return c.json(result, 200);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    logger.error('Failed to delete dataset', { correlationId, name, error: errorMessage });
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

const startCreateRoute = createRoute({
  method: 'post',
  path: '/api/dataset',
  tags: ['Dataset'],
  summary: 'Start dataset creation',
  description:
    'Start a new dataset creation job. Only one creation job can run at a time. Returns immediately with job ID for polling.',
  request: {
    body: {
      content: {
        'application/json': {
          schema: DatasetCreateRequestSchema,
        },
      },
    },
  },
  responses: {
    202: {
      content: {
        'application/json': {
          schema: CreateDatasetJobResponseSchema,
        },
      },
      description: 'Dataset creation job started',
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
      description: 'Another creation job is already running or dataset already exists',
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

datasetApp.openapi(startCreateRoute, async (c) => {
  const body = c.req.valid('json');
  const correlationId = getCorrelationId(c);

  logger.info('Dataset creation job requested', { correlationId, name: body.name, preset: body.preset });

  try {
    const result = datasetService.startCreateJob(body.name, body.preset, body.overwrite, body.timeoutMinutes);

    if (!result) {
      return c.json(
        createErrorResponse({
          error: 'Conflict',
          message:
            'Another creation job is already running or dataset already exists (use overwrite: true to replace).',
          correlationId,
        }),
        409
      );
    }

    return c.json(result, 202);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    logger.error('Failed to start dataset creation job', { correlationId, error: errorMessage });
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

const resumeDatasetRoute = createRoute({
  method: 'post',
  path: '/api/dataset/resume',
  tags: ['Dataset'],
  summary: 'Resume incomplete dataset',
  description:
    'Resume fetching missing data for an existing dataset. Detects and fetches only missing quotes/statements/margin data.',
  request: {
    body: {
      content: {
        'application/json': {
          schema: DatasetCreateRequestSchema,
        },
      },
    },
  },
  responses: {
    202: {
      content: {
        'application/json': {
          schema: CreateDatasetJobResponseSchema,
        },
      },
      description: 'Dataset resume job started',
    },
    400: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Invalid request parameters',
    },
    404: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Dataset not found',
    },
    409: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Another job is already running',
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

datasetApp.openapi(resumeDatasetRoute, async (c) => {
  const body = c.req.valid('json');
  const correlationId = getCorrelationId(c);

  logger.info('Dataset resume job requested', { correlationId, name: body.name, preset: body.preset });

  try {
    const result = datasetService.startResumeJob(body.name, body.preset, body.timeoutMinutes);

    if (!result) {
      return c.json(
        createErrorResponse({
          error: 'Conflict',
          message: 'Another job is already running or dataset does not exist.',
          correlationId,
        }),
        409
      );
    }

    return c.json(result, 202);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    logger.error('Failed to start dataset resume job', { correlationId, error: errorMessage });
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

const getJobStatusRoute = createRoute({
  method: 'get',
  path: '/api/dataset/jobs/{jobId}',
  tags: ['Dataset'],
  summary: 'Get dataset creation job status',
  description: 'Get the current status and progress of a dataset creation job.',
  request: {
    params: z.object({
      jobId: z.string().uuid().openapi({ description: 'Job ID', example: '123e4567-e89b-12d3-a456-426614174000' }),
    }),
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: DatasetJobResponseSchema,
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

datasetApp.openapi(getJobStatusRoute, async (c) => {
  const { jobId } = c.req.valid('param');
  const correlationId = getCorrelationId(c);

  const job = datasetService.getJobStatus(jobId);

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

const cancelJobRoute = createRoute({
  method: 'delete',
  path: '/api/dataset/jobs/{jobId}',
  tags: ['Dataset'],
  summary: 'Cancel dataset creation job',
  description: 'Cancel a running or pending dataset creation job.',
  request: {
    params: z.object({
      jobId: z.string().uuid().openapi({ description: 'Job ID', example: '123e4567-e89b-12d3-a456-426614174000' }),
    }),
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: CancelDatasetJobResponseSchema,
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

datasetApp.openapi(cancelJobRoute, async (c) => {
  const { jobId } = c.req.valid('param');
  const correlationId = getCorrelationId(c);

  logger.info('Dataset job cancellation requested', { correlationId, jobId });

  const result = datasetService.cancelJob(jobId);

  if (!result.success) {
    const job = datasetService.getJobStatus(jobId);
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

  return c.json(result, 200);
});

const getInfoRoute = createRoute({
  method: 'get',
  path: '/api/dataset/{name}/info',
  tags: ['Dataset'],
  summary: 'Get dataset information',
  description: 'Get information about an existing dataset including statistics.',
  request: {
    params: z.object({
      name: z.string().min(1).openapi({ description: 'Dataset name', example: 'prime.db' }),
    }),
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: DatasetInfoResponseSchema,
        },
      },
      description: 'Dataset information',
    },
    404: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Dataset not found',
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

datasetApp.openapi(getInfoRoute, async (c) => {
  const { name } = c.req.valid('param');
  const correlationId = getCorrelationId(c);

  try {
    const info = await datasetService.getDatasetInfo(name);

    if (!info) {
      return c.json(
        createErrorResponse({
          error: 'Not Found',
          message: `Dataset "${name}" not found`,
          correlationId,
        }),
        404
      );
    }

    return c.json(info, 200);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    logger.error('Failed to get dataset info', { correlationId, name, error: errorMessage });
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

const sampleRoute = createRoute({
  method: 'get',
  path: '/api/dataset/{name}/sample',
  tags: ['Dataset'],
  summary: 'Sample stocks from dataset',
  description: 'Get a random sample of stock codes from the dataset.',
  request: {
    params: z.object({
      name: z.string().min(1).openapi({ description: 'Dataset name', example: 'prime.db' }),
    }),
    query: DatasetSampleQuerySchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: DatasetSampleResponseSchema,
        },
      },
      description: 'Sampled stock codes',
    },
    404: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Dataset not found',
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

datasetApp.openapi(sampleRoute, async (c) => {
  const { name } = c.req.valid('param');
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  try {
    const result = await datasetService.sampleDataset(name, query);

    if (!result) {
      return c.json(
        createErrorResponse({
          error: 'Not Found',
          message: `Dataset "${name}" not found`,
          correlationId,
        }),
        404
      );
    }

    return c.json(result, 200);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    logger.error('Failed to sample dataset', { correlationId, name, error: errorMessage });
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

const searchRoute = createRoute({
  method: 'get',
  path: '/api/dataset/{name}/search',
  tags: ['Dataset'],
  summary: 'Search stocks in dataset',
  description: 'Search for stocks by code or company name.',
  request: {
    params: z.object({
      name: z.string().min(1).openapi({ description: 'Dataset name', example: 'prime.db' }),
    }),
    query: DatasetSearchQuerySchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: DatasetSearchResponseSchema,
        },
      },
      description: 'Search results',
    },
    404: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Dataset not found',
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

datasetApp.openapi(searchRoute, async (c) => {
  const { name } = c.req.valid('param');
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  try {
    const result = await datasetService.searchDataset(name, query);

    if (!result) {
      return c.json(
        createErrorResponse({
          error: 'Not Found',
          message: `Dataset "${name}" not found`,
          correlationId,
        }),
        404
      );
    }

    return c.json(result, 200);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    logger.error('Failed to search dataset', { correlationId, name, error: errorMessage });
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

export default datasetApp;
