import { logger } from '@trading25/shared/utils/logger';

/**
 * Interface for services that require cleanup on shutdown
 */
export interface CleanupableService {
  close(): void;
}

/**
 * Registry of services that need cleanup on shutdown
 */
const serviceRegistry = new Map<string, CleanupableService>();

/**
 * Flag to track if cleanup handlers have been registered
 */
let handlersRegistered = false;

/**
 * Cleanup timeout in milliseconds
 */
const CLEANUP_TIMEOUT_MS = 5000;

/**
 * Cleanup all registered services with timeout protection
 */
function cleanupAllServices(): void {
  if (serviceRegistry.size === 0) {
    return;
  }

  logger.info(`Cleaning up ${serviceRegistry.size} services...`);

  for (const [name, service] of serviceRegistry) {
    try {
      logger.info(`Closing ${name}...`);
      service.close();
    } catch (error) {
      logger.error(`Failed to close ${name}`, {
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }
  serviceRegistry.clear();
  logger.info('All services cleaned up');
}

/**
 * Register shutdown handlers if not already registered
 */
function ensureHandlersRegistered(): void {
  if (handlersRegistered) return;

  const handleShutdown = (signal: string) => {
    logger.info(`Received ${signal}, cleaning up services...`);

    // Set a timeout to force exit if cleanup takes too long
    const forceExitTimeout = setTimeout(() => {
      logger.warn(`Cleanup timeout (${CLEANUP_TIMEOUT_MS}ms) exceeded, forcing exit`);
      process.exit(1);
    }, CLEANUP_TIMEOUT_MS);

    // Don't let the timeout prevent natural exit
    forceExitTimeout.unref();

    try {
      cleanupAllServices();
    } finally {
      clearTimeout(forceExitTimeout);
      logger.info('Shutdown complete');
      process.exit(0);
    }
  };

  process.on('SIGINT', () => handleShutdown('SIGINT'));
  process.on('SIGTERM', () => handleShutdown('SIGTERM'));

  handlersRegistered = true;
}

/**
 * Register a service for cleanup on shutdown
 *
 * Registers the service to be closed when the process receives SIGINT or SIGTERM.
 * Duplicate registrations with the same name will replace the previous service.
 *
 * @param name - Unique name for the service (used for logging)
 * @param service - Service instance with a close() method
 *
 * @example
 * ```typescript
 * const marketRankingService = new MarketRankingService();
 * registerServiceForCleanup('MarketRankingService', marketRankingService);
 * ```
 */
export function registerServiceForCleanup(name: string, service: CleanupableService): void {
  ensureHandlersRegistered();
  serviceRegistry.set(name, service);
}

/**
 * Unregister a service from cleanup
 *
 * Call this if you manually close a service before shutdown.
 *
 * @param name - Name of the service to unregister
 */
export function unregisterService(name: string): void {
  serviceRegistry.delete(name);
}

/**
 * Configuration for managed service creation
 */
export interface ManagedServiceConfig<T extends CleanupableService> {
  /** Factory function to create the service */
  factory: () => T;
  /** Optional setup function called after service creation */
  setup?: (service: T) => void;
}

/**
 * Create a managed service with automatic lifecycle handling
 *
 * Creates a singleton service instance that is automatically registered
 * for cleanup on shutdown. Provides lazy initialization.
 *
 * @param name - Unique name for the service
 * @param config - Service configuration
 * @returns Function that returns the service instance (lazy initialization)
 *
 * @example
 * ```typescript
 * const getMarketRankingService = createManagedService('MarketRankingService', {
 *   factory: () => new MarketRankingService(),
 * });
 *
 * // In route handler:
 * const service = getMarketRankingService();
 * const data = await service.getRankings(params);
 * ```
 */
export function createManagedService<T extends CleanupableService>(
  name: string,
  config: ManagedServiceConfig<T>
): () => T {
  let instance: T | null = null;

  return () => {
    if (!instance) {
      instance = config.factory();
      registerServiceForCleanup(name, instance);
      config.setup?.(instance);
    }
    return instance;
  };
}

/**
 * Get the number of registered services (useful for testing)
 */
export function getRegisteredServiceCount(): number {
  return serviceRegistry.size;
}

/**
 * Clear all registered services without calling close (useful for testing)
 */
export function clearServiceRegistry(): void {
  serviceRegistry.clear();
}
