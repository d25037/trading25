/**
 * Format Helpers
 * Common formatting utilities for CLI commands
 */

import chalk from 'chalk';

/**
 * Format elapsed time in human-readable format
 */
export function formatElapsedTime(ms: number): string {
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds % 60;

  if (minutes > 0) {
    return `${minutes}m ${remainingSeconds}s`;
  }
  return `${seconds}s`;
}

/**
 * Format file size in human readable format
 */
export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / k ** i).toFixed(1)} ${sizes[i]}`;
}

/**
 * Format data coverage as colored string based on percentage thresholds
 */
export function formatCoverage(covered: number, total: number): string {
  if (total === 0) return chalk.gray('N/A');

  const percentage = (covered / total) * 100;
  const text = `${covered}/${total} (${percentage >= 100 ? '100' : percentage.toFixed(1)}%)`;

  if (percentage >= 100) return chalk.green(text);
  if (percentage >= 90) return chalk.yellow(text);
  return chalk.red(text);
}

/**
 * Format availability status for boolean flags
 */
export function formatAvailability(isAvailable: boolean): string {
  return isAvailable ? chalk.green('✓ Available') : chalk.gray('✗ Not available');
}

/**
 * Sleep utility for polling operations
 */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Log debug message if debug mode is enabled
 */
export function logDebug(debug: boolean, message: string): void {
  if (debug) console.log(chalk.gray(`[DEBUG] ${message}`));
}
