import type { ILogger, LogContext, LogLevel } from '@trading25/shared/utils/logger-interface';

class BrowserLogger implements ILogger {
  private level: LogLevel;

  constructor() {
    // ブラウザ環境でのログレベル設定
    const isDev = window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1';
    this.level = isDev ? 'DEBUG' : 'WARN';
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

  private formatMessage(_level: LogLevel, message: string, context?: LogContext): string {
    const correlationId = context?.correlationId;
    const prefix = correlationId ? `[${correlationId.slice(0, 8)}]` : '';
    return `${prefix} ${message}`;
  }

  private log(level: LogLevel, message: string, context?: LogContext): void {
    if (!this.shouldLog(level)) return;

    const formattedMessage = this.formatMessage(level, message, context);

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
  }

  createCorrelationId(): string {
    return crypto.randomUUID();
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
}

export const logger = new BrowserLogger();
export default logger;
