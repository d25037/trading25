import type { TokenData, TokenStorage } from '../utils/token-storage';

export interface AuthCredentials {
  apiKey?: string;
}

export interface AuthStatus {
  authenticated: boolean;
  hasApiKey: boolean;
}

/**
 * Unified authentication service for JQuants API v2
 * Works across CLI and frontend environments using TokenStorage interface
 */
export class AuthService {
  private tokenStorage: TokenStorage;

  constructor(tokenStorage: TokenStorage) {
    this.tokenStorage = tokenStorage;
  }

  /**
   * Authenticate with JQuants API using stored or provided API key
   */
  async authenticate(credentials?: AuthCredentials): Promise<void> {
    // Get stored tokens first
    const storedTokens = await this.tokenStorage.getTokens();

    // Merge provided credentials with stored tokens
    const finalApiKey = credentials?.apiKey || storedTokens.apiKey;

    if (!finalApiKey) {
      throw new Error('API key is required for authentication');
    }

    // Save API key to storage
    await this.tokenStorage.saveTokens({
      apiKey: finalApiKey,
    });
  }

  /**
   * Get current authentication status
   */
  async getAuthStatus(): Promise<AuthStatus> {
    const tokens = await this.tokenStorage.getTokens();

    return {
      authenticated: !!tokens.apiKey,
      hasApiKey: !!tokens.apiKey,
    };
  }

  /**
   * Direct JQuants client access has been removed.
   * Use bt FastAPI endpoints (`/api/jquants/*`) via API client classes.
   */
  async getClient(): Promise<never> {
    throw new Error('Direct J-Quants client is removed. Use bt FastAPI endpoints (/api/jquants/*).');
  }

  /**
   * Clear all stored tokens
   */
  async clearTokens(): Promise<void> {
    await this.tokenStorage.clearTokens();
  }

  /**
   * Check if valid API key exists in storage
   */
  async hasValidTokens(): Promise<boolean> {
    return this.tokenStorage.hasValidTokens();
  }

  /**
   * Get stored tokens
   */
  async getStoredTokens(): Promise<TokenData> {
    return this.tokenStorage.getTokens();
  }

  /**
   * Save credentials to storage
   */
  async saveCredentials(credentials: AuthCredentials): Promise<void> {
    await this.tokenStorage.saveTokens(credentials);
  }

  /**
   * Check if token storage is available
   */
  isStorageAvailable(): boolean {
    return this.tokenStorage.isAvailable();
  }
}
