import type { JQuantsClient } from '@trading25/shared';
import { createJQuantsClient } from '../utils/jquants-client-factory';

/**
 * Base service class for JQuants API services
 * Provides common functionality for client creation and error handling
 */
export abstract class BaseJQuantsService {
  protected jquantsClient: JQuantsClient | null = null;

  /**
   * Create and configure JQuants client
   * Default implementation uses the centralized factory function
   */
  protected createJQuantsClient(): JQuantsClient {
    return createJQuantsClient();
  }

  /**
   * Get or create JQuants client instance
   */
  protected getJQuantsClient(): JQuantsClient {
    if (!this.jquantsClient) {
      this.jquantsClient = this.createJQuantsClient();
    }
    return this.jquantsClient;
  }

  /**
   * Check if error is an authentication error
   */
  protected isAuthError(error: unknown): boolean {
    if (!(error instanceof Error)) {
      return false;
    }

    const message = error.message.toLowerCase();
    return (
      message.includes('authentication') ||
      message.includes('unauthorized') ||
      message.includes('token') ||
      message.includes('401')
    );
  }

  /**
   * Execute a function (no token refresh needed in API v2)
   * @param fn Function to execute
   * @returns Result of the function
   */
  protected async withTokenRefresh<T>(fn: () => Promise<T>): Promise<T> {
    return await fn();
  }
}
