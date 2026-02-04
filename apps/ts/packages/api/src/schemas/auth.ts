import { z } from '@hono/zod-openapi';

/**
 * Auth status response schema - JQuants API v2
 */
export const AuthStatusResponseSchema = z
  .object({
    authenticated: z.boolean().openapi({
      description: 'Whether API key is configured',
    }),
    hasApiKey: z.boolean().openapi({
      description: 'Whether an API key is configured',
    }),
  })
  .openapi('AuthStatusResponse', {
    description: 'JQuants API v2 authentication status',
  });
