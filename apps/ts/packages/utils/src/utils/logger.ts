import { randomUUID } from 'node:crypto';
import type { ILogger, LogContext, LogLevel } from './logger-interface';

export interface LogEntry {
  level: LogLevel;
  message: string;
  timestamp: string;
  context?: LogContext;
}

let hasReportedBrowserDetectionError = false;

function reportBrowserDetectionError(error: unknown): void {
  if (process.env.NODE_ENV === 'test' || hasReportedBrowserDetectionError) {
    return;
  }
  hasReportedBrowserDetectionError = true;
  console.warn(
    `[shared logger] Browser environment detection failed; defaulting to server logger mode: ${error instanceof Error ? error.message : String(error)}`
  );
}

class LoggerImpl implements ILogger {
  private level: LogLevel;
  private isBrowser: boolean;
  private correlationId?: string;

  constructor() {
    this.isBrowser = this.detectBrowser();
    this.level = this.getLogLevel();
  }

  private detectBrowser(): boolean {
    try {
      // Check if we're in a browser environment
      return typeof globalThis !== 'undefined' && 'window' in globalThis && 'document' in globalThis;
    } catch (error) {
      reportBrowserDetectionError(error);
      return false;
    }
  }

  private getLogLevel(): LogLevel {
    const envLevel = process.env.NODE_ENV;
    // Convert to uppercase to support both 'debug' and 'DEBUG' in .env
    const logLevel = process.env.LOG_LEVEL?.toUpperCase() as LogLevel;

    if (logLevel && this.isValidLogLevel(logLevel)) {
      return logLevel;
    }

    switch (envLevel) {
      case 'test':
        return 'SILENT';
      case 'production':
        return 'WARN';
      default:
        return 'INFO';
    }
  }

  private isValidLogLevel(level: string): level is LogLevel {
    return ['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL', 'SILENT'].includes(level);
  }

  private getLevelPriority(level: LogLevel): number {
    const priorities = {
      TRACE: 0,
      DEBUG: 1,
      INFO: 2,
      WARN: 3,
      ERROR: 4,
      FATAL: 5,
      SILENT: 6,
    };
    return priorities[level];
  }

  private shouldLog(level: LogLevel): boolean {
    return this.getLevelPriority(level) >= this.getLevelPriority(this.level);
  }

  private formatMessage(level: LogLevel, message: string, context?: LogContext): string {
    const timestamp = new Date().toISOString();
    const correlationId = context?.correlationId || this.correlationId;

    if (this.isBrowser) {
      const prefix = correlationId ? `[${correlationId.slice(0, 8)}]` : '';
      return `${prefix} ${message}`;
    }

    if (process.env.NODE_ENV === 'production') {
      return JSON.stringify({
        timestamp,
        level,
        message,
        correlationId,
        ...context,
      });
    }

    const colorMap = {
      TRACE: '\x1b[37m',
      DEBUG: '\x1b[36m',
      INFO: '\x1b[32m',
      WARN: '\x1b[33m',
      ERROR: '\x1b[31m',
      FATAL: '\x1b[35m',
      SILENT: '\x1b[0m',
    };

    const reset = '\x1b[0m';
    const color = colorMap[level];
    const prefix = correlationId ? `[${correlationId.slice(0, 8)}]` : '';

    return `${color}[${level}]${reset} ${prefix} ${message}`;
  }

  private log(level: LogLevel, message: string, context?: LogContext): void {
    if (!this.shouldLog(level)) return;

    const formattedMessage = this.formatMessage(level, message, context);

    if (this.isBrowser) {
      const colorMap = {
        TRACE: 'color: #888',
        DEBUG: 'color: #0ff',
        INFO: 'color: #0a0',
        WARN: 'color: #fa0',
        ERROR: 'color: #f00',
        FATAL: 'color: #f0f',
        SILENT: '',
      };

      if (level === 'ERROR' || level === 'FATAL') {
        console.error(`%c${formattedMessage}`, colorMap[level]);
      } else if (level === 'WARN') {
        console.warn(`%c${formattedMessage}`, colorMap[level]);
      } else {
        console.log(`%c${formattedMessage}`, colorMap[level]);
      }
    } else {
      if (level === 'ERROR' || level === 'FATAL') {
        console.error(formattedMessage);
      } else if (level === 'WARN') {
        console.warn(formattedMessage);
      } else {
        console.log(formattedMessage);
      }
    }
  }

  setCorrelationId(id: string | undefined): void {
    this.correlationId = id;
  }

  createCorrelationId(): string {
    return randomUUID();
  }

  trace(message: string, context?: LogContext): void {
    this.log('TRACE', message, context);
  }

  debug(message: string, context?: LogContext): void {
    this.log('DEBUG', message, context);
  }

  info(message: string, context?: LogContext): void {
    this.log('INFO', message, context);
  }

  warn(message: string, context?: LogContext): void {
    this.log('WARN', message, context);
  }

  error(message: string, context?: LogContext): void {
    this.log('ERROR', message, context);
  }

  fatal(message: string, context?: LogContext): void {
    this.log('FATAL', message, context);
  }

  child(context: LogContext): LoggerImpl {
    const childLogger = new LoggerImpl();
    childLogger.correlationId = context.correlationId || this.correlationId;
    return childLogger;
  }
}

export const logger = new LoggerImpl();
export default logger;

// Backward compatibility - Legacy Logger interface
export interface Logger {
  trace(message: string, data?: unknown): void;
  debug(message: string, data?: unknown): void;
  info(message: string, data?: unknown): void;
  warn(message: string, data?: unknown): void;
  error(message: string, data?: unknown): void;
  fatal(message: string, data?: unknown): void;
}

// Backward compatibility - Legacy logger implementations
export class ConsoleLogger implements Logger {
  trace(message: string, data?: unknown): void {
    logger.trace(message, data as LogContext);
  }
  debug(message: string, data?: unknown): void {
    logger.debug(message, data as LogContext);
  }
  info(message: string, data?: unknown): void {
    logger.info(message, data as LogContext);
  }
  warn(message: string, data?: unknown): void {
    logger.warn(message, data as LogContext);
  }
  error(message: string, data?: unknown): void {
    logger.error(message, data as LogContext);
  }
  fatal(message: string, data?: unknown): void {
    logger.fatal(message, data as LogContext);
  }
}

export class SilentLogger implements Logger {
  trace(): void {}
  debug(): void {}
  info(): void {}
  warn(): void {}
  error(): void {}
  fatal(): void {}
}

export function createDefaultLogger(): Logger {
  return new ConsoleLogger();
}
