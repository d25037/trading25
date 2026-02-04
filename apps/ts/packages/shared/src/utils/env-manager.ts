import * as fs from 'node:fs';
import * as path from 'node:path';
import { EnvContentProcessor, parseEnvValue } from './env-manager-helpers';

export interface EnvTokens {
  JQUANTS_API_KEY?: string;
}

export class EnvManager {
  private envPath: string;

  constructor(envPath?: string) {
    this.envPath = envPath || path.resolve(process.cwd(), '.env');
  }

  /**
   * Read current .env file content
   */
  readEnvFile(): string {
    if (!fs.existsSync(this.envPath)) {
      throw new Error(`Environment file not found: ${this.envPath}`);
    }
    return fs.readFileSync(this.envPath, 'utf-8');
  }

  /**
   * Parse .env content into key-value pairs
   */
  parseEnvContent(content: string): Record<string, string> {
    const env: Record<string, string> = {};
    const lines = content.split('\n');

    for (const line of lines) {
      const trimmed = line.trim();
      if (trimmed && !trimmed.startsWith('#')) {
        const equalIndex = trimmed.indexOf('=');
        if (equalIndex > 0) {
          const key = trimmed.substring(0, equalIndex).trim();
          const rawValue = trimmed.substring(equalIndex + 1).trim();
          env[key] = parseEnvValue(rawValue);
        }
      }
    }

    return env;
  }

  /**
   * Update specific tokens in .env file
   */
  updateTokens(tokens: EnvTokens): void {
    try {
      // Create backup
      this.createBackup();

      const content = this.readEnvFile();
      const env = this.parseEnvContent(content);

      // Update API key
      if (tokens.JQUANTS_API_KEY !== undefined) {
        env.JQUANTS_API_KEY = tokens.JQUANTS_API_KEY;
      }

      // Write updated content
      const updatedContent = this.generateEnvContent(content, env);
      fs.writeFileSync(this.envPath, updatedContent, 'utf-8');
    } catch (error) {
      // Restore backup on error
      this.restoreBackup();
      throw error;
    }
  }

  /**
   * Generate updated .env content preserving comments and formatting
   */
  private generateEnvContent(originalContent: string, updatedEnv: Record<string, string>): string {
    const processor = new EnvContentProcessor(updatedEnv);
    const lines = originalContent.split('\n');
    return processor.processLines(lines);
  }

  /**
   * Create backup of current .env file
   */
  private createBackup(): void {
    if (fs.existsSync(this.envPath)) {
      const backupPath = `${this.envPath}.backup`;
      fs.copyFileSync(this.envPath, backupPath);
    }
  }

  /**
   * Restore from backup
   */
  private restoreBackup(): void {
    const backupPath = `${this.envPath}.backup`;
    if (fs.existsSync(backupPath)) {
      fs.copyFileSync(backupPath, this.envPath);
      fs.unlinkSync(backupPath);
    }
  }

  /**
   * Clean up backup file
   */
  cleanupBackup(): void {
    const backupPath = `${this.envPath}.backup`;
    if (fs.existsSync(backupPath)) {
      fs.unlinkSync(backupPath);
    }
  }

  /**
   * Check if .env file exists
   */
  exists(): boolean {
    return fs.existsSync(this.envPath);
  }

  /**
   * Get current tokens from .env
   */
  getCurrentTokens(): EnvTokens {
    if (!this.exists()) {
      return {};
    }

    const content = this.readEnvFile();
    const env = this.parseEnvContent(content);

    return {
      JQUANTS_API_KEY: env.JQUANTS_API_KEY,
    };
  }
}
