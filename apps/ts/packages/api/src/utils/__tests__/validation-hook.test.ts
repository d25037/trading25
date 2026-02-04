import { describe, expect, test } from 'bun:test';
import { createOpenAPIApp } from '../validation-hook';

describe('createOpenAPIApp', () => {
  test('creates an OpenAPIHono instance', () => {
    const app = createOpenAPIApp();
    expect(app).toBeDefined();
    expect(typeof app.request).toBe('function');
  });

  test('returns 400 on validation failure', async () => {
    const { createRoute, z } = await import('@hono/zod-openapi');

    const app = createOpenAPIApp();

    const route = createRoute({
      method: 'get',
      path: '/test',
      request: {
        query: z.object({
          required_param: z.string(),
        }),
      },
      responses: {
        200: {
          content: { 'application/json': { schema: z.object({ ok: z.boolean() }) } },
          description: 'OK',
        },
        400: {
          content: { 'application/json': { schema: z.object({}) } },
          description: 'Bad request',
        },
      },
    });

    app.openapi(route, (c) => {
      return c.json({ ok: true }, 200);
    });

    const res = await app.request('/test');
    expect(res.status).toBe(400);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.status).toBe('error');
    expect(body.error).toBe('Bad Request');
    expect(body.message).toBe('Request validation failed');
    expect(body.details).toBeDefined();
    expect(Array.isArray(body.details)).toBe(true);
  });
});
