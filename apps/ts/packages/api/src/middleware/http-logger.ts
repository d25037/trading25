import { isTrading25Error } from '@trading25/shared';
import { logger } from '@trading25/shared/utils/logger';
import type { MiddlewareHandler } from 'hono';
import { createErrorResponse } from '../utils/error-responses';
import { correlationMiddleware, getCorrelationId } from './correlation';

export const httpLogger = (): MiddlewareHandler => {
  return async (c, next) => {
    const start = Date.now();
    const method = c.req.method;
    const path = c.req.path;

    try {
      await next();

      const end = Date.now();
      const status = c.res.status;
      const elapsed = end - start;
      const correlationId = getCorrelationId(c);

      logger.info(`${method} ${path} ${status} ${elapsed}ms`, {
        correlationId,
        method,
        path,
        status,
        elapsed,
      });
    } catch (error) {
      const end = Date.now();
      const elapsed = end - start;
      const correlationId = getCorrelationId(c);

      logger.error(`${method} ${path} ERROR ${elapsed}ms`, {
        correlationId,
        method,
        path,
        error: error instanceof Error ? error.message : String(error),
        elapsed,
      });
      throw error;
    }
  };
};

export const requestLogger = (): MiddlewareHandler[] => {
  return [correlationMiddleware, httpLogger()];
};

type HttpErrorType = 'Bad Request' | 'Not Found' | 'Conflict' | 'Internal Server Error';

function getHttpStatusText(status: number): HttpErrorType {
  switch (status) {
    case 400:
      return 'Bad Request';
    case 404:
      return 'Not Found';
    case 409:
      return 'Conflict';
    default:
      return 'Internal Server Error';
  }
}

export const errorHandler = (): MiddlewareHandler => {
  return async (c, next) => {
    try {
      await next();
    } catch (error) {
      const correlationId = getCorrelationId(c);

      // Handle Trading25Error subclasses with proper HTTP status codes
      if (isTrading25Error(error)) {
        logger.warn('Request error', {
          correlationId,
          errorCode: error.code,
          httpStatus: error.httpStatus,
          message: error.message,
        });

        return c.json(
          createErrorResponse({
            error: getHttpStatusText(error.httpStatus),
            message: error.message,
            correlationId,
          }),
          error.httpStatus as 400 | 404 | 409 | 500
        );
      }

      // Handle unexpected errors
      logger.error('Unhandled error', {
        correlationId,
        error: error instanceof Error ? error.message : String(error),
        stack: error instanceof Error ? error.stack : undefined,
      });

      return c.json(
        createErrorResponse({
          error: 'Internal Server Error',
          message: error instanceof Error ? error.message : 'An unknown error occurred',
          correlationId,
        }),
        500
      );
    }
  };
};
