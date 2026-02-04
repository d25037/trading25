import { FileTokenStorage, TokenManager } from '@trading25/shared';
import chalk from 'chalk';

/**
 * CLI-specific token manager factory
 * Creates a TokenManager with file-based storage and CLI logging
 */
export function createCliTokenManager(envPath?: string): TokenManager {
  const storage = new FileTokenStorage({
    envPath,
    logger: (message: string, level?: 'info' | 'warn' | 'error') => {
      switch (level) {
        case 'error':
          console.error(chalk.red(`❌ ${message}`));
          break;
        case 'warn':
          console.log(chalk.yellow(`Warning: ${message}`));
          break;
        case 'info':
          console.log(chalk.green(`✅ ${message}`));
          break;
        default:
          console.log(message);
      }
    },
  });

  return new TokenManager(storage);
}

/**
 * CLI token manager with display utilities for JQuants API v2
 * Manages API key storage and display for CLI-specific functionality
 */
export class CLITokenManager {
  private tokenManager: TokenManager;

  constructor(envPath?: string) {
    this.tokenManager = createCliTokenManager(envPath);
  }

  /**
   * Save API key to .env file
   */
  async saveApiKey(apiKey: string): Promise<void> {
    await this.tokenManager.saveTokens({ apiKey });
  }

  /**
   * Get API key from .env file
   */
  async getApiKey(): Promise<string | undefined> {
    const tokens = await this.tokenManager.getTokens();
    return tokens.apiKey;
  }

  /**
   * Clear API key from .env file
   */
  async clearTokens(): Promise<void> {
    await this.tokenManager.clearTokens();
  }

  /**
   * Check if .env file has valid API key
   */
  async hasValidTokens(): Promise<boolean> {
    return this.tokenManager.hasValidTokens();
  }

  /**
   * Display current API key status from .env
   */
  async displayStatus(): Promise<void> {
    const apiKey = await this.getApiKey();
    console.log(chalk.cyan('\n🔐 JQuants API v2 Status (.env file)'));
    console.log(chalk.white('━'.repeat(50)));
    console.log(chalk.yellow('Has API Key:'), apiKey ? chalk.green('Yes') : chalk.red('No'));
    if (apiKey) {
      // Only show first/last 4 chars for security
      const masked = `${apiKey.slice(0, 4)}...${apiKey.slice(-4)}`;
      console.log(chalk.yellow('API Key:'), chalk.gray(masked));
    }
  }
}
