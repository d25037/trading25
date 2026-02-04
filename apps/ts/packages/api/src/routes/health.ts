import { createRoute, OpenAPIHono, z } from '@hono/zod-openapi';

const healthApp = new OpenAPIHono();

/**
 * Health check response schema
 */
const HealthResponseSchema = z
  .object({
    status: z.literal('ok'),
    timestamp: z.string().datetime(),
  })
  .openapi('HealthResponse', {
    description: 'Health check response indicating service is operational',
  });

/**
 * Health check endpoint route definition
 */
const healthRoute = createRoute({
  method: 'get',
  path: '/health',
  tags: ['Health'],
  summary: 'Health check',
  description: 'Check if the API service is running and responsive',
  responses: {
    200: {
      content: {
        'application/json': {
          schema: HealthResponseSchema,
        },
      },
      description: 'Service is healthy',
    },
  },
});

/**
 * Health check endpoint handler
 */
healthApp.openapi(healthRoute, (c) => {
  return c.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
  });
});

export default healthApp;
