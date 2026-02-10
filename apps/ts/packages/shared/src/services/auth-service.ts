import { JQuantsClient } from '@trading25/clients-ts/JQuantsClient';
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
  private client: JQuantsClient | null = null;

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

    // Create client with API key
    this.client = new JQuantsClient({
      apiKey: finalApiKey,
    });

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
   * Get authenticated JQuants client
   * Automatically creates and authenticates client if needed
   */
  async getClient(): Promise<JQuantsClient> {
    if (!this.client) {
      await this.authenticate();
    }

    if (!this.client) {
      throw new Error('Failed to create authenticated client');
    }

    return this.client;
  }

  /**
   * Clear all stored tokens
   */
  async clearTokens(): Promise<void> {
    await this.tokenStorage.clearTokens();
    this.client = null;
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
