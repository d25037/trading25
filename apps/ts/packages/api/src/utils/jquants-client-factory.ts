/**
 * JQuantsClient Factory
 * Unified factory function for creating JQuantsClient instances with proper authentication
 */

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { JQuantsClient } from '@trading25/shared/clients/JQuantsClient';

// Cache for loaded env values
let envLoaded = false;
const envValues: Record<string, string> = {};

/**
 * Get the project root .env path
 * Uses import.meta.url to find the current file location, then navigate to project root
 */
function getEnvPath(): string {
  // Get the directory of this file
  const currentDir = path.dirname(fileURLToPath(import.meta.url));
  // From packages/api/src/utils, go up 4 levels to reach project root
  return path.resolve(currentDir, '..', '..', '..', '..', '.env');
}

/**
 * Parse a single .env line and return key-value pair
 */
function parseEnvLine(line: string): { key: string; value: string } | null {
  const trimmedLine = line.trim();
  // Skip empty lines and comments
  if (!trimmedLine || trimmedLine.startsWith('#')) return null;

  const [key, ...valueParts] = trimmedLine.split('=');
  if (!key || valueParts.length === 0) return null;

  const value = valueParts.join('=').trim();
  // Remove surrounding quotes if present
  const cleanValue = value.replace(/^["']|["']$/g, '');
  // Remove inline comments (only for unquoted values)
  const hasQuotes = (value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"));
  let finalValue = cleanValue;
  if (!hasQuotes) {
    const commentIndex = cleanValue.indexOf(' #');
    if (commentIndex !== -1) {
      finalValue = cleanValue.substring(0, commentIndex).trim();
    }
  }
  return { key: key.trim(), value: finalValue };
}

/**
 * Load environment variables from .env file if not already loaded by Bun
 * This is needed when running from subpackages where Bun doesn't auto-load root .env
 */
function loadEnvFile(): void {
  if (envLoaded) return;
  envLoaded = true;

  const envPath = getEnvPath();
  if (!fs.existsSync(envPath)) return;

  try {
    const content = fs.readFileSync(envPath, 'utf-8');
    for (const line of content.split('\n')) {
      const parsed = parseEnvLine(line);
      if (!parsed) continue;

      envValues[parsed.key] = parsed.value;
      // Also set in process.env if not already set
      if (!process.env[parsed.key]) {
        process.env[parsed.key] = parsed.value;
      }
    }
  } catch (error) {
    console.error('[JQuantsClientFactory] Failed to load .env file:', error);
  }
}

/**
 * Get environment variable value, checking both process.env and loaded .env
 */
function getEnvValue(key: string): string | undefined {
  loadEnvFile();
  return process.env[key] || envValues[key];
}

/**
 * Create a properly configured JQuantsClient instance
 * Reads credentials from environment variables
 */
export function createJQuantsClient(): JQuantsClient {
  // Ensure env is loaded
  loadEnvFile();

  const apiKey = getEnvValue('JQUANTS_API_KEY') || '';

  return new JQuantsClient({
    apiKey,
  });
}
