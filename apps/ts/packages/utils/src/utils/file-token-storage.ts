import { EnvManager, type EnvTokens } from './env-manager';
import type { TokenData, TokenStorage, TokenStorageOptions } from './token-storage';

/**
 * File-based token storage using .env files
 * For Node.js environments (CLI, server) - stores API key for JQuants v2
 */
export class FileTokenStorage implements TokenStorage {
  private envManager: EnvManager;
  private logger?: (message: string, level?: 'info' | 'warn' | 'error') => void;

  constructor(options?: TokenStorageOptions) {
    this.envManager = new EnvManager(options?.envPath);
    this.logger = options?.logger;
  }

  async saveTokens(tokens: TokenData): Promise<void> {
    try {
      if (!this.envManager.exists()) {
        this.logger?.('Warning: .env file not found, skipping .env update', 'warn');
        return;
      }

      const envTokens: EnvTokens = {};
      if (tokens.apiKey) envTokens.JQUANTS_API_KEY = tokens.apiKey;

      this.envManager.updateTokens(envTokens);
      this.logger?.('Updated .env file with API key', 'info');
    } catch (error) {
      const message = `Failed to update .env file: ${error instanceof Error ? error.message : error}`;
      this.logger?.(message, 'error');
      throw new Error(message);
    }
  }

  async getTokens(): Promise<TokenData> {
    const envTokens = this.envManager.getCurrentTokens();
    return {
      apiKey: envTokens.JQUANTS_API_KEY,
    };
  }

  async clearTokens(): Promise<void> {
    try {
      this.envManager.updateTokens({
        JQUANTS_API_KEY: '',
      });
      this.logger?.('Cleared API key from .env file', 'info');
    } catch (error) {
      const message = `Failed to clear API key: ${error instanceof Error ? error.message : error}`;
      this.logger?.(message, 'error');
      throw new Error(message);
    }
  }

  async hasValidTokens(): Promise<boolean> {
    const tokens = await this.getTokens();
    return !!tokens.apiKey;
  }

  isAvailable(): boolean {
    return this.envManager.exists();
  }
}
