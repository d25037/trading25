import type { TokenData, TokenStorage } from './token-storage';

/**
 * Unified token manager that works across environments
 * Uses TokenStorage interface for environment-agnostic token management
 */
export class TokenManager {
  private storage: TokenStorage;

  constructor(storage: TokenStorage) {
    this.storage = storage;
  }

  /**
   * Save tokens to storage
   */
  async saveTokens(tokens: TokenData): Promise<void> {
    await this.storage.saveTokens(tokens);
  }

  /**
   * Get tokens from storage
   */
  async getTokens(): Promise<TokenData> {
    return this.storage.getTokens();
  }

  /**
   * Clear tokens from storage
   */
  async clearTokens(): Promise<void> {
    await this.storage.clearTokens();
  }

  /**
   * Check if storage has valid tokens
   */
  async hasValidTokens(): Promise<boolean> {
    return this.storage.hasValidTokens();
  }

  /**
   * Check if storage is available
   */
  isStorageAvailable(): boolean {
    return this.storage.isAvailable();
  }
}
