import type { Hook } from '@hono/zod-openapi';
import { OpenAPIHono } from '@hono/zod-openapi';
import type { ZodError } from 'zod';
import { createErrorResponse } from './error-responses';

/**
 * Shared validation error hook for OpenAPI routes
 *
 * Automatically handles Zod validation errors and returns unified error format
 * with correlation ID tracking. This hook is used as the defaultHook for
 * OpenAPIHono instances to ensure consistent error responses across all routes.
 *
 * @example
 * ```typescript
 * import { validationHook } from '../../utils/validation-hook';
 *
 * const app = new OpenAPIHono({ defaultHook: validationHook });
 * ```
 */
// biome-ignore lint/suspicious/noExplicitAny: Hook type requires any for flexibility with various route schemas
export const validationHook: Hook<any, any, any, any> = (result, c) => {
  if (!result.success) {
    const zodError = result.error as ZodError;
    const correlationId = c.get('correlationId') || c.req.header('x-correlation-id') || crypto.randomUUID();

    return c.json(
      createErrorResponse({
        error: 'Bad Request',
        message: 'Request validation failed',
        details: zodError.issues.map((issue) => ({
          field: issue.path.join('.'),
          message: issue.message,
        })),
        correlationId,
      }),
      400
    );
  }
};

/**
 * Create a new OpenAPIHono instance with the shared validation hook pre-configured
 *
 * This factory function ensures all route apps use consistent validation error handling.
 *
 * @returns OpenAPIHono instance with defaultHook set to validationHook
 *
 * @example
 * ```typescript
 * import { createOpenAPIApp } from '../../utils/validation-hook';
 *
 * const myApp = createOpenAPIApp();
 *
 * myApp.openapi(myRoute, async (c) => {
 *   // handler logic
 * });
 *
 * export default myApp;
 * ```
 */
export function createOpenAPIApp(): OpenAPIHono {
  return new OpenAPIHono({
    defaultHook: validationHook,
  });
}
