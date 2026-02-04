/**
 * Common logger interface for type safety across environments
 * Provides a unified logging interface that can be implemented by both
 * Node.js and browser-specific loggers while maintaining environment compatibility
 */

export type LogLevel = 'TRACE' | 'DEBUG' | 'INFO' | 'WARN' | 'ERROR' | 'FATAL' | 'SILENT';

export interface LogContext {
  correlationId?: string;
  component?: string;
  method?: string;
  [key: string]: unknown;
}

/**
 * Common logger interface that both Node.js and browser loggers implement
 */
export interface ILogger {
  trace(message: string, context?: LogContext): void;
  debug(message: string, context?: LogContext): void;
  info(message: string, context?: LogContext): void;
  warn(message: string, context?: LogContext): void;
  error(message: string, context?: LogContext): void;
  fatal(message: string, context?: LogContext): void;
  createCorrelationId(): string;
}

/**
 * Logger factory for creating environment-appropriate loggers
 */
export interface ILoggerFactory {
  createLogger(): ILogger;
  createChildLogger(context: LogContext): ILogger;
}
