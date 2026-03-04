/**
 * Helper functions for EnvManager to reduce complexity
 */

/**
 * Parse a raw .env value string, removing quotes and inline comments
 */
export function parseEnvValue(rawValue: string): string {
  const isQuoted =
    (rawValue.startsWith('"') && rawValue.endsWith('"')) || (rawValue.startsWith("'") && rawValue.endsWith("'"));

  if (isQuoted) {
    return rawValue.slice(1, -1);
  }

  // Remove inline comments for unquoted values
  const commentIndex = rawValue.indexOf(' #');
  if (commentIndex !== -1) {
    return rawValue.substring(0, commentIndex).trim();
  }

  return rawValue;
}

export class EnvContentProcessor {
  private processedKeys = new Set<string>();
  private updatedLines: string[] = [];

  constructor(private updatedEnv: Record<string, string>) {}

  processLines(lines: string[]): string {
    this.processedKeys.clear();
    this.updatedLines = [];

    for (const line of lines) {
      this.processLine(line);
    }

    this.addNewKeys();
    return this.updatedLines.join('\n');
  }

  private processLine(line: string): void {
    const trimmed = line.trim();

    if (this.isCommentOrEmpty(trimmed)) {
      this.updatedLines.push(line);
      return;
    }

    this.processEnvLine(line, trimmed);
  }

  private isCommentOrEmpty(trimmed: string): boolean {
    return !trimmed || trimmed.startsWith('#');
  }

  private processEnvLine(line: string, trimmed: string): void {
    const equalIndex = trimmed.indexOf('=');

    if (equalIndex <= 0) {
      this.updatedLines.push(line);
      return;
    }

    const key = trimmed.substring(0, equalIndex).trim();

    if (this.shouldUpdateKey(key)) {
      this.updateExistingKey(key);
    } else {
      this.updatedLines.push(line);
    }
  }

  private shouldUpdateKey(key: string): boolean {
    return key in this.updatedEnv;
  }

  private updateExistingKey(key: string): void {
    const value = this.updatedEnv[key];
    if (value !== undefined) {
      const quotedValue = this.shouldQuoteValue(value) ? `'${value}'` : value;
      this.updatedLines.push(`${key}=${quotedValue}`);
      this.processedKeys.add(key);
    }
  }

  private shouldQuoteValue(value: string): boolean {
    return value.includes(' ') || value.includes('\t') || value.includes('\n');
  }

  private addNewKeys(): void {
    for (const [key, value] of Object.entries(this.updatedEnv)) {
      if (!this.processedKeys.has(key)) {
        const quotedValue = this.shouldQuoteValue(value) ? `'${value}'` : value;
        this.updatedLines.push(`${key}=${quotedValue}`);
      }
    }
  }
}
