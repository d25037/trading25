export interface TokenData {
  apiKey?: string;
}

export interface TokenStorage {
  /**
   * Save tokens to storage
   */
  saveTokens(tokens: TokenData): Promise<void>;

  /**
   * Get tokens from storage
   */
  getTokens(): Promise<TokenData>;

  /**
   * Clear tokens from storage
   */
  clearTokens(): Promise<void>;

  /**
   * Check if valid tokens exist
   */
  hasValidTokens(): Promise<boolean>;

  /**
   * Check if storage is available
   */
  isAvailable(): boolean;
}

export interface TokenStorageOptions {
  envPath?: string;
  logger?: (message: string, level?: 'info' | 'warn' | 'error') => void;
}
