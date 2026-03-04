/**
 * Secure Environment Manager
 * Simple AES-256-GCM encryption for JQuants tokens using Node.js standard crypto
 */

import * as crypto from 'node:crypto';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { EnvManager, type EnvTokens } from './env-manager';

/**
 * Encryption configuration
 */
interface EncryptionConfig {
  algorithm: string;
  keyLength: number;
  ivLength: number;
  tagLength: number;
}

const DEFAULT_ENCRYPTION_CONFIG: EncryptionConfig = {
  algorithm: 'aes-256-gcm',
  keyLength: 32, // 256 bits
  ivLength: 12, // 96 bits for GCM
  tagLength: 16, // 128 bits
};

/**
 * Encrypted token structure
 */
interface EncryptedToken {
  encrypted: string;
  iv: string;
  tag: string;
  algorithm: string;
}

/**
 * Secure environment manager with token encryption
 */
export class SecureEnvManager {
  private envManager: EnvManager;
  private encryptionKey: Buffer | null = null;
  private config: EncryptionConfig;
  private hasReportedTokenParseFailure = false;

  constructor(
    envPath?: string,
    private keyPath?: string,
    config?: Partial<EncryptionConfig>
  ) {
    this.envManager = new EnvManager(envPath);
    this.config = { ...DEFAULT_ENCRYPTION_CONFIG, ...config };
    this.keyPath = keyPath || path.resolve(process.cwd(), '.trading25.key');
  }

  /**
   * Get the resolved key path, ensuring it's always available
   */
  private getKeyPath(): string {
    if (!this.keyPath) {
      throw new Error('Key path is not initialized');
    }
    return this.keyPath;
  }

  /**
   * Initialize encryption key (generate if doesn't exist)
   */
  async initializeEncryption(): Promise<void> {
    try {
      const keyPath = this.getKeyPath();
      if (fs.existsSync(keyPath)) {
        // Load existing key
        const keyData = fs.readFileSync(keyPath, 'utf-8');
        this.encryptionKey = Buffer.from(keyData, 'hex');

        if (this.encryptionKey.length !== this.config.keyLength) {
          throw new Error(`Invalid key length: expected ${this.config.keyLength}, got ${this.encryptionKey.length}`);
        }
      } else {
        // Generate new key
        this.encryptionKey = crypto.randomBytes(this.config.keyLength);
        fs.writeFileSync(keyPath, this.encryptionKey.toString('hex'), { mode: 0o600 });
        console.warn(`⚠️  New encryption key generated at: ${keyPath}`);
        console.warn(`🔐 Keep this key file secure and do not commit it to version control`);
      }
    } catch (error) {
      throw new Error(`Failed to initialize encryption: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Encrypt a token string
   */
  private encryptToken(token: string): EncryptedToken {
    if (!this.encryptionKey) {
      throw new Error('Encryption not initialized. Call initializeEncryption() first.');
    }

    const iv = crypto.randomBytes(this.config.ivLength);
    const cipher = crypto.createCipheriv(
      this.config.algorithm as crypto.CipherGCMTypes,
      this.encryptionKey as crypto.CipherKey,
      iv as crypto.BinaryLike
    ) as crypto.CipherGCM;

    let encrypted = cipher.update(token, 'utf8', 'hex');
    encrypted += cipher.final('hex');

    const tag = cipher.getAuthTag();

    return {
      encrypted,
      iv: iv.toString('hex'),
      tag: tag.toString('hex'),
      algorithm: this.config.algorithm,
    };
  }

  /**
   * Decrypt a token
   */
  private decryptToken(encryptedToken: EncryptedToken): string {
    if (!this.encryptionKey) {
      throw new Error('Encryption not initialized. Call initializeEncryption() first.');
    }

    if (encryptedToken.algorithm !== this.config.algorithm) {
      throw new Error(`Algorithm mismatch: expected ${this.config.algorithm}, got ${encryptedToken.algorithm}`);
    }

    const iv = Buffer.from(encryptedToken.iv, 'hex');
    const tag = Buffer.from(encryptedToken.tag, 'hex');
    const decipher = crypto.createDecipheriv(
      encryptedToken.algorithm as crypto.CipherGCMTypes,
      this.encryptionKey as crypto.CipherKey,
      iv as crypto.BinaryLike
    ) as crypto.DecipherGCM;
    decipher.setAuthTag(tag);

    let decrypted = decipher.update(encryptedToken.encrypted, 'hex', 'utf8');
    decrypted += decipher.final('utf8');

    return decrypted;
  }

  /**
   * Check if a value is an encrypted token
   */
  private isEncryptedToken(value: string): boolean {
    try {
      const parsed = JSON.parse(value);
      return (
        typeof parsed === 'object' &&
        parsed !== null &&
        'encrypted' in parsed &&
        'iv' in parsed &&
        'tag' in parsed &&
        'algorithm' in parsed
      );
    } catch (error) {
      if (process.env.NODE_ENV !== 'test' && !this.hasReportedTokenParseFailure) {
        this.hasReportedTokenParseFailure = true;
        console.warn(
          `[SecureEnvManager] Failed to parse encrypted token payload; treating as plain text: ${error instanceof Error ? error.message : String(error)}`
        );
      }
      return false;
    }
  }

  /**
   * Securely store API key with encryption
   */
  async storeTokensSecurely(tokens: EnvTokens): Promise<void> {
    if (!this.encryptionKey) {
      await this.initializeEncryption();
    }

    const encryptedTokens: EnvTokens = {};

    // Encrypt API key
    if (tokens.JQUANTS_API_KEY) {
      const encrypted = this.encryptToken(tokens.JQUANTS_API_KEY);
      encryptedTokens.JQUANTS_API_KEY = JSON.stringify(encrypted);
    }

    // Store using the regular env manager
    this.envManager.updateTokens(encryptedTokens);
  }

  /**
   * Retrieve and decrypt API key
   */
  async getTokensSecurely(): Promise<EnvTokens> {
    if (!this.encryptionKey) {
      await this.initializeEncryption();
    }

    const storedTokens = this.envManager.getCurrentTokens();
    const decryptedTokens: EnvTokens = {};

    // Decrypt API key if encrypted
    if (storedTokens.JQUANTS_API_KEY) {
      if (this.isEncryptedToken(storedTokens.JQUANTS_API_KEY)) {
        const encrypted = JSON.parse(storedTokens.JQUANTS_API_KEY) as EncryptedToken;
        decryptedTokens.JQUANTS_API_KEY = this.decryptToken(encrypted);
      } else {
        // Plain text API key
        decryptedTokens.JQUANTS_API_KEY = storedTokens.JQUANTS_API_KEY;
      }
    }

    return decryptedTokens;
  }

  /**
   * Migrate existing plain text API key to encrypted format
   */
  async migrateToEncryption(): Promise<{ migrated: number; skipped: number }> {
    if (!this.encryptionKey) {
      await this.initializeEncryption();
    }

    const currentTokens = this.envManager.getCurrentTokens();
    let migrated = 0;
    let skipped = 0;

    const tokensToMigrate: EnvTokens = {};

    // Check API key and migrate if it's plain text
    if (currentTokens.JQUANTS_API_KEY) {
      if (!this.isEncryptedToken(currentTokens.JQUANTS_API_KEY)) {
        // Plain text API key - encrypt it
        const encrypted = this.encryptToken(currentTokens.JQUANTS_API_KEY);
        tokensToMigrate.JQUANTS_API_KEY = JSON.stringify(encrypted);
        migrated++;
      } else {
        skipped++;
      }
    }

    if (migrated > 0) {
      this.envManager.updateTokens(tokensToMigrate);
      console.log(`🔐 Migrated ${migrated} API key to encrypted format`);
    }

    return { migrated, skipped };
  }

  /**
   * Verify that encryption is working correctly
   */
  async verifyEncryption(): Promise<boolean> {
    try {
      if (!this.encryptionKey) {
        await this.initializeEncryption();
      }

      // Test encryption/decryption with a sample token
      const testToken = 'test-token-12345';
      const encrypted = this.encryptToken(testToken);
      const decrypted = this.decryptToken(encrypted);

      return decrypted === testToken;
    } catch (error) {
      console.error('Encryption verification failed:', error);
      return false;
    }
  }

  /**
   * Get encryption status
   */
  async getEncryptionStatus(): Promise<{
    isInitialized: boolean;
    keyExists: boolean;
    tokensEncrypted: { [key: string]: boolean };
  }> {
    const status = {
      isInitialized: this.encryptionKey !== null,
      keyExists: fs.existsSync(this.getKeyPath()),
      tokensEncrypted: {} as { [key: string]: boolean },
    };

    if (status.keyExists) {
      const currentTokens = this.envManager.getCurrentTokens();

      if (currentTokens.JQUANTS_API_KEY) {
        status.tokensEncrypted.JQUANTS_API_KEY = this.isEncryptedToken(currentTokens.JQUANTS_API_KEY);
      }
    }

    return status;
  }

  /**
   * Cleanup - secure deletion of encryption key (use with caution)
   */
  async deleteEncryptionKey(): Promise<void> {
    const keyPath = this.getKeyPath();
    if (fs.existsSync(keyPath)) {
      // Overwrite the key file with random data before deletion
      const keySize = fs.statSync(keyPath).size;
      const randomData = crypto.randomBytes(keySize);
      fs.writeFileSync(keyPath, randomData as unknown as string);
      fs.unlinkSync(keyPath);

      this.encryptionKey = null;
      console.warn(`🗑️  Encryption key deleted: ${keyPath}`);
    }
  }

  /**
   * Get the underlying env manager for compatibility
   */
  getEnvManager(): EnvManager {
    return this.envManager;
  }
}
