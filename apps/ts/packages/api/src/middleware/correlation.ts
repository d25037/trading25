import { logger } from '@trading25/shared/utils/logger';
import type { Context, Next } from 'hono';

export const CORRELATION_ID_HEADER = 'x-correlation-id';

export interface CorrelationContext {
  correlationId: string;
}

declare module 'hono' {
  interface ContextVariableMap {
    correlationId: string;
  }
}

export const correlationMiddleware = async (c: Context, next: Next) => {
  const correlationId = c.req.header(CORRELATION_ID_HEADER) || logger.createCorrelationId();

  c.set('correlationId', correlationId);
  c.header(CORRELATION_ID_HEADER, correlationId);

  logger.setCorrelationId(correlationId);

  await next();

  logger.setCorrelationId(undefined);
};

export function getCorrelationId(c: Context): string {
  return c.get('correlationId');
}

export function createCorrelationId(): string {
  return logger.createCorrelationId();
}
