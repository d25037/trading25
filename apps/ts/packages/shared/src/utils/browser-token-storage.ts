import type { TokenData, TokenStorage, TokenStorageOptions } from './token-storage';

/**
 * Minimal localStorage interface to avoid depending on DOM types
 */
interface LocalStorageInterface {
  setItem(key: string, value: string): void;
  getItem(key: string): string | null;
  removeItem(key: string): void;
}

/**
 * Browser-based token storage using localStorage
 * For frontend environments - stores API key for JQuants v2
 */
export class BrowserTokenStorage implements TokenStorage {
  private keyPrefix: string;
  private logger?: (message: string, level?: 'info' | 'warn' | 'error') => void;
  private availabilityErrorLogged = false;

  constructor(options?: TokenStorageOptions & { keyPrefix?: string }) {
    this.keyPrefix = options?.keyPrefix || 'trading25_';
    this.logger = options?.logger;
  }

  async saveTokens(tokens: TokenData): Promise<void> {
    try {
      if (!this.isAvailable()) {
        this.logger?.('localStorage not available', 'warn');
        return;
      }

      const storage = (globalThis as unknown as { localStorage: LocalStorageInterface }).localStorage;
      if (tokens.apiKey) {
        storage.setItem(`${this.keyPrefix}api_key`, tokens.apiKey);
      }

      this.logger?.('Updated localStorage with API key', 'info');
    } catch (error) {
      const message = `Failed to save API key to localStorage: ${error instanceof Error ? error.message : error}`;
      this.logger?.(message, 'error');
      throw new Error(message);
    }
  }

  async getTokens(): Promise<TokenData> {
    if (!this.isAvailable()) {
      return {};
    }

    const storage = (globalThis as unknown as { localStorage: LocalStorageInterface }).localStorage;
    return {
      apiKey: storage.getItem(`${this.keyPrefix}api_key`) || undefined,
    };
  }

  async clearTokens(): Promise<void> {
    try {
      if (!this.isAvailable()) {
        return;
      }

      const storage = (globalThis as unknown as { localStorage: LocalStorageInterface }).localStorage;
      storage.removeItem(`${this.keyPrefix}api_key`);

      this.logger?.('Cleared API key from localStorage', 'info');
    } catch (error) {
      const message = `Failed to clear API key from localStorage: ${error instanceof Error ? error.message : error}`;
      this.logger?.(message, 'error');
      throw new Error(message);
    }
  }

  async hasValidTokens(): Promise<boolean> {
    const tokens = await this.getTokens();
    return !!tokens.apiKey;
  }

  isAvailable(): boolean {
    try {
      return (
        typeof globalThis !== 'undefined' &&
        'localStorage' in globalThis &&
        (globalThis as unknown as { localStorage: LocalStorageInterface }).localStorage !== null
      );
    } catch (error) {
      if (!this.availabilityErrorLogged) {
        this.availabilityErrorLogged = true;
        this.logger?.(
          `localStorage availability check failed: ${error instanceof Error ? error.message : String(error)}`,
          'warn'
        );
      }
      return false;
    }
  }
}
