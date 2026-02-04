/**
 * Application configuration with environment variable support
 */

export interface DatabaseConfig {
  maxConnections: number;
  statementCacheSize: number;
  walCheckpointInterval: number;
  queryTimeout: number;
}

export interface RateLimiterConfig {
  requestsPerSecond: number;
  maxRequestHistory: number;
  absoluteMaxHistory: number;
  circuitBreakerFailureThreshold: number;
  circuitBreakerResetTimeoutMs: number;
}

export interface DatasetConfig {
  defaultChunkSize: number;
  progressReportInterval: number;
  retryAttempts: number;
}

export interface AppConfig {
  database: DatabaseConfig;
  rateLimiter: RateLimiterConfig;
  dataset: DatasetConfig;
  logLevel: 'debug' | 'info' | 'warn' | 'error';
  isDevelopment: boolean;
  isTest: boolean;
}

/**
 * Default configuration values
 */
const DEFAULT_CONFIG: AppConfig = {
  database: {
    maxConnections: 3,
    statementCacheSize: 100,
    walCheckpointInterval: 5000,
    queryTimeout: 30000,
  },
  rateLimiter: {
    requestsPerSecond: 10,
    maxRequestHistory: 1000,
    absoluteMaxHistory: 5000,
    circuitBreakerFailureThreshold: 5,
    circuitBreakerResetTimeoutMs: 60000,
  },
  dataset: {
    defaultChunkSize: 100,
    progressReportInterval: 20,
    retryAttempts: 3,
  },
  logLevel: 'info',
  isDevelopment: process.env.NODE_ENV === 'development',
  isTest: process.env.NODE_ENV === 'test',
};

/**
 * Parse numeric environment variable with fallback
 */
function parseNumber(value: string | undefined, fallback: number): number {
  if (!value) return fallback;
  const parsed = Number(value);
  return Number.isNaN(parsed) ? fallback : parsed;
}

/**
 * Parse log level with validation
 */
function parseLogLevel(value: string | undefined): 'debug' | 'info' | 'warn' | 'error' {
  if (!value) return 'info';
  const level = value.toLowerCase();
  if (['debug', 'info', 'warn', 'error'].includes(level)) {
    return level as 'debug' | 'info' | 'warn' | 'error';
  }
  return 'info';
}

/**
 * Load configuration from environment variables with defaults
 */
function loadConfig(): AppConfig {
  return {
    database: {
      maxConnections: parseNumber(process.env.DB_MAX_CONNECTIONS, DEFAULT_CONFIG.database.maxConnections),
      statementCacheSize: parseNumber(process.env.DB_STATEMENT_CACHE_SIZE, DEFAULT_CONFIG.database.statementCacheSize),
      walCheckpointInterval: parseNumber(
        process.env.DB_WAL_CHECKPOINT_INTERVAL,
        DEFAULT_CONFIG.database.walCheckpointInterval
      ),
      queryTimeout: parseNumber(process.env.DB_QUERY_TIMEOUT, DEFAULT_CONFIG.database.queryTimeout),
    },
    rateLimiter: {
      requestsPerSecond: parseNumber(process.env.RATE_LIMIT_RPS, DEFAULT_CONFIG.rateLimiter.requestsPerSecond),
      maxRequestHistory: parseNumber(process.env.RATE_LIMIT_MAX_HISTORY, DEFAULT_CONFIG.rateLimiter.maxRequestHistory),
      absoluteMaxHistory: parseNumber(
        process.env.RATE_LIMIT_ABSOLUTE_MAX_HISTORY,
        DEFAULT_CONFIG.rateLimiter.absoluteMaxHistory
      ),
      circuitBreakerFailureThreshold: parseNumber(
        process.env.CIRCUIT_BREAKER_THRESHOLD,
        DEFAULT_CONFIG.rateLimiter.circuitBreakerFailureThreshold
      ),
      circuitBreakerResetTimeoutMs: parseNumber(
        process.env.CIRCUIT_BREAKER_RESET_TIMEOUT,
        DEFAULT_CONFIG.rateLimiter.circuitBreakerResetTimeoutMs
      ),
    },
    dataset: {
      defaultChunkSize: parseNumber(process.env.DATASET_CHUNK_SIZE, DEFAULT_CONFIG.dataset.defaultChunkSize),
      progressReportInterval: parseNumber(
        process.env.DATASET_PROGRESS_INTERVAL,
        DEFAULT_CONFIG.dataset.progressReportInterval
      ),
      retryAttempts: parseNumber(process.env.DATASET_RETRY_ATTEMPTS, DEFAULT_CONFIG.dataset.retryAttempts),
    },
    logLevel: parseLogLevel(process.env.LOG_LEVEL),
    isDevelopment: process.env.NODE_ENV === 'development',
    isTest: process.env.NODE_ENV === 'test',
  };
}

/**
 * Singleton configuration instance
 */
let configInstance: AppConfig | null = null;

/**
 * Get application configuration
 */
export function getConfig(): AppConfig {
  if (!configInstance) {
    configInstance = loadConfig();
  }
  return configInstance;
}

/**
 * Reset configuration (mainly for testing)
 */
export function resetConfig(): void {
  configInstance = null;
}

/**
 * Override specific configuration values (mainly for testing)
 */
export function setConfig(overrides: Partial<AppConfig>): void {
  configInstance = {
    ...getConfig(),
    ...overrides,
  };
}
