import { z } from '@hono/zod-openapi';

/**
 * Sync mode enum
 */
export const SyncModeSchema = z.enum(['auto', 'initial', 'incremental', 'indices-only']).openapi({
  description:
    'Sync mode: auto (detect based on database state), initial (2 years), incremental, or indices-only (52 API calls)',
  example: 'auto',
});

/**
 * Market sync request body schema
 */
export const MarketSyncRequestSchema = z
  .object({
    mode: SyncModeSchema.default('auto').openapi({
      description: 'Sync mode (default: auto)',
      example: 'auto',
    }),
  })
  .openapi('MarketSyncRequest');

/**
 * Job status enum
 */
export const JobStatusSchema = z.enum(['pending', 'running', 'completed', 'failed', 'cancelled']).openapi({
  description: 'Current job status',
  example: 'running',
});

/**
 * Job progress schema
 */
export const JobProgressSchema = z
  .object({
    stage: z.string().openapi({ description: 'Current stage name', example: 'Fetching daily quotes' }),
    current: z.number().int().openapi({ description: 'Current progress count', example: 50 }),
    total: z.number().int().openapi({ description: 'Total items to process', example: 252 }),
    percentage: z.number().openapi({ description: 'Progress percentage', example: 19.84 }),
    message: z.string().openapi({ description: 'Progress message', example: 'Processing 2024-01-15' }),
  })
  .openapi('JobProgress');

/**
 * Sync job result schema
 */
export const SyncJobResultSchema = z
  .object({
    success: z.boolean().openapi({ description: 'Whether sync was successful', example: true }),
    totalApiCalls: z.number().int().openapi({ description: 'Total API calls made', example: 252 }),
    stocksUpdated: z.number().int().openapi({ description: 'Number of stocks updated', example: 1800 }),
    datesProcessed: z.number().int().openapi({ description: 'Number of dates processed', example: 250 }),
    failedDates: z.array(z.string()).openapi({ description: 'Failed dates (YYYY-MM-DD format)', example: [] }),
    errors: z.array(z.string()).openapi({ description: 'Error messages', example: [] }),
  })
  .openapi('SyncJobResult');

/**
 * Sync job response schema
 */
export const SyncJobResponseSchema = z
  .object({
    jobId: z
      .string()
      .uuid()
      .openapi({ description: 'Unique job identifier', example: '123e4567-e89b-12d3-a456-426614174000' }),
    status: JobStatusSchema,
    mode: SyncModeSchema,
    progress: JobProgressSchema.optional(),
    result: SyncJobResultSchema.optional(),
    startedAt: z.string().datetime().openapi({ description: 'Job start time' }),
    completedAt: z.string().datetime().optional().openapi({ description: 'Job completion time' }),
    error: z.string().optional().openapi({ description: 'Error message if failed' }),
  })
  .openapi('SyncJobResponse', {
    description: 'Sync job status and progress',
  });

/**
 * Create sync job response schema
 */
export const CreateSyncJobResponseSchema = z
  .object({
    jobId: z
      .string()
      .uuid()
      .openapi({ description: 'Created job ID', example: '123e4567-e89b-12d3-a456-426614174000' }),
    status: JobStatusSchema,
    mode: SyncModeSchema,
    estimatedApiCalls: z.number().int().openapi({ description: 'Estimated API calls', example: 252 }),
    message: z.string().openapi({ description: 'Status message', example: 'Sync job started' }),
  })
  .openapi('CreateSyncJobResponse', {
    description: 'Response when creating a new sync job',
  });

/**
 * Cancel job response schema
 */
export const CancelJobResponseSchema = z
  .object({
    success: z.boolean().openapi({ description: 'Whether cancellation was successful', example: true }),
    jobId: z.string().uuid().openapi({ description: 'Cancelled job ID' }),
    message: z.string().openapi({ description: 'Status message', example: 'Job cancelled successfully' }),
  })
  .openapi('CancelJobResponse');

/**
 * Type exports
 */
export type SyncMode = z.infer<typeof SyncModeSchema>;
export type MarketSyncRequest = z.infer<typeof MarketSyncRequestSchema>;
export type JobStatus = z.infer<typeof JobStatusSchema>;
export type JobProgress = z.infer<typeof JobProgressSchema>;
export type SyncJobResult = z.infer<typeof SyncJobResultSchema>;
export type SyncJobResponse = z.infer<typeof SyncJobResponseSchema>;
export type CreateSyncJobResponse = z.infer<typeof CreateSyncJobResponseSchema>;
export type CancelJobResponse = z.infer<typeof CancelJobResponseSchema>;
