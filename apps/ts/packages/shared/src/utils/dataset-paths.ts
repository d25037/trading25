/**
 * Dataset path utilities for consistent dataset directory handling
 */

import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { findProjectRoot } from './find-project-root';

/**
 * Default dataset directory name relative to project root
 */
export const DEFAULT_DATASET_DIR = 'dataset';

/**
 * Get the default dataset directory path (project-root/dataset)
 */
export function getDefaultDatasetDir(): string {
  const projectRoot = findProjectRoot();
  return path.join(projectRoot, DEFAULT_DATASET_DIR);
}

/**
 * Ensure the dataset directory exists, creating it if necessary
 */
export function ensureDatasetDir(): string {
  const datasetDir = getDefaultDatasetDir();

  if (!fs.existsSync(datasetDir)) {
    try {
      fs.mkdirSync(datasetDir, { recursive: true });
    } catch (error) {
      throw new Error(
        `Failed to create dataset directory at ${datasetDir}: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  return datasetDir;
}

/**
 * Resolve dataset path with security-first handling:
 * - All paths are restricted to root/dataset/ directory for security
 * - Simple filename (e.g., "prime.db") -> project-root/dataset/prime.db
 * - Relative path (e.g., "subdir/data.db") -> project-root/dataset/subdir/data.db
 * - Absolute paths are rejected for security
 */
export function resolveDatasetPath(input: string): string {
  // Validate input
  if (!input || typeof input !== 'string') {
    throw new Error('Invalid file path provided: path must be a non-empty string');
  }

  // Security: Reject absolute paths - all files must be within dataset directory
  if (path.isAbsolute(input)) {
    throw new Error(
      `Absolute paths are not allowed for security reasons: "${input}". Use relative paths within the dataset directory.`
    );
  }

  // Security: Validate that the input doesn't contain path traversal attempts
  if (input.includes('..')) {
    throw new Error(
      `Parent directory references (..) are not allowed for security: "${input}". All files must be within the dataset directory.`
    );
  }

  // Validate filename length (reasonable limit)
  if (input.length > 255) {
    throw new Error(`Path too long: "${input}". Maximum length is 255 characters.`);
  }

  // Security: Validate against invalid filesystem characters
  const invalidChars = /[<>:"|?*]/;
  const hasControlChars = input.split('').some((char) => {
    const code = char.charCodeAt(0);
    return code >= 0 && code <= 31;
  });
  if (invalidChars.test(input) || hasControlChars) {
    throw new Error(`Invalid characters in path: "${input}". Path contains forbidden filesystem characters.`);
  }

  // Remove leading ./ if present (normalize relative paths)
  const normalizedInput = input.startsWith('./') ? input.slice(2) : input;

  // Ensure the dataset directory exists
  const datasetDir = ensureDatasetDir();

  // All paths go into the dataset directory
  const resolvedPath = path.join(datasetDir, normalizedInput);

  // Security: Verify the resolved path is still within the dataset directory
  // This prevents any edge cases or platform-specific path traversal
  const normalizedDatasetDir = path.resolve(datasetDir);
  const normalizedResolvedPath = path.resolve(resolvedPath);

  if (
    !normalizedResolvedPath.startsWith(normalizedDatasetDir + path.sep) &&
    normalizedResolvedPath !== normalizedDatasetDir
  ) {
    throw new Error(
      `Resolved path is outside dataset directory: "${input}". All files must be within the dataset directory.`
    );
  }

  return resolvedPath;
}

/**
 * Ensure the path has .db extension for SQLite databases
 */
export function ensureDbExtension(filePath: string): string {
  // Add validation for empty or invalid inputs
  if (!filePath || typeof filePath !== 'string') {
    throw new Error('Invalid file path provided: path must be a non-empty string');
  }

  if (!filePath.endsWith('.db')) {
    return `${filePath}.db`;
  }
  return filePath;
}

/**
 * Resolve and normalize dataset path with .db extension
 * This is the main function that should be used by CLI commands
 * All paths are restricted to root/dataset/ directory for security
 */
export function normalizeDatasetPath(input: string): string {
  const resolvedPath = resolveDatasetPath(input);
  const finalPath = ensureDbExtension(resolvedPath);

  // Ensure parent directory exists and is writable
  const parentDir = path.dirname(finalPath);

  // Create parent directory if it doesn't exist (within dataset directory)
  if (!fs.existsSync(parentDir)) {
    try {
      fs.mkdirSync(parentDir, { recursive: true });
    } catch (error) {
      throw new Error(
        `Failed to create directory: ${parentDir}. ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  // Check if parent directory is writable
  try {
    fs.accessSync(parentDir, fs.constants.W_OK);
  } catch (_error) {
    throw new Error(`Cannot write to directory: ${parentDir}`);
  }

  return finalPath;
}

/**
 * Get the market database path in the user's data directory
 * Follows XDG Base Directory specification for cross-project shared data
 *
 * Path resolution:
 * - Uses XDG_DATA_HOME if set (e.g., /custom/path/trading25/market.db)
 * - Otherwise uses $HOME/.local/share/trading25/market.db
 *
 * The directory is automatically created if it doesn't exist.
 *
 * @returns Absolute path to market.db
 */
export function getMarketDbPath(): string {
  // Use XDG_DATA_HOME if set, otherwise default to $HOME/.local/share
  const dataHome = process.env.XDG_DATA_HOME || path.join(os.homedir(), '.local', 'share');
  const tradingDataDir = path.join(dataHome, 'trading25');

  // Ensure the directory exists
  if (!fs.existsSync(tradingDataDir)) {
    try {
      fs.mkdirSync(tradingDataDir, { recursive: true });
    } catch (error) {
      throw new Error(
        `Failed to create trading25 data directory at ${tradingDataDir}: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  return path.join(tradingDataDir, 'market.db');
}

/**
 * Get the portfolio database path (XDG-compliant)
 *
 * Returns the path to the portfolio database file used for tracking stock holdings.
 * Follows XDG Base Directory specification for cross-project shared data.
 *
 * Path resolution:
 * - Uses XDG_DATA_HOME if set (e.g., /custom/path/trading25/portfolio.db)
 * - Otherwise uses $HOME/.local/share/trading25/portfolio.db
 *
 * The directory is automatically created if it doesn't exist.
 *
 * @returns Absolute path to portfolio.db
 */
export function getPortfolioDbPath(): string {
  // Use XDG_DATA_HOME if set, otherwise default to $HOME/.local/share
  const dataHome = process.env.XDG_DATA_HOME || path.join(os.homedir(), '.local', 'share');
  const tradingDataDir = path.join(dataHome, 'trading25');

  // Ensure the directory exists
  if (!fs.existsSync(tradingDataDir)) {
    try {
      fs.mkdirSync(tradingDataDir, { recursive: true });
    } catch (error) {
      throw new Error(
        `Failed to create trading25 data directory at ${tradingDataDir}: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  return path.join(tradingDataDir, 'portfolio.db');
}

/**
 * Validate dataset filename for security and format requirements
 */
function validateDatasetFilename(filename: string): void {
  if (!filename || typeof filename !== 'string') {
    throw new Error('Invalid filename provided: must be a non-empty string');
  }

  if (path.isAbsolute(filename)) {
    throw new Error(
      `Absolute paths are not allowed for security reasons: "${filename}". Use relative paths within the datasets directory.`
    );
  }

  if (filename.includes('..')) {
    throw new Error(
      `Parent directory references (..) are not allowed for security: "${filename}". All files must be within the datasets directory.`
    );
  }

  if (filename.length > 255) {
    throw new Error(`Path too long: "${filename}". Maximum length is 255 characters.`);
  }

  const invalidChars = /[<>:"|?*]/;
  const hasControlChars = filename.split('').some((char) => {
    const code = char.charCodeAt(0);
    return code >= 0 && code <= 31;
  });

  if (invalidChars.test(filename) || hasControlChars) {
    throw new Error(`Invalid characters in filename: "${filename}". Path contains forbidden filesystem characters.`);
  }
}

/**
 * Verify that resolved path stays within datasets directory
 */
function verifyPathBounds(finalPath: string, datasetsBaseDir: string, filename: string): void {
  const normalizedDatasetsDir = path.resolve(datasetsBaseDir);
  const normalizedFinalPath = path.resolve(finalPath);

  if (
    !normalizedFinalPath.startsWith(normalizedDatasetsDir + path.sep) &&
    normalizedFinalPath !== normalizedDatasetsDir
  ) {
    throw new Error(
      `Resolved path is outside datasets directory: "${filename}". All files must be within the datasets directory.`
    );
  }
}

/**
 * Get the dataset database path in the user's data directory
 * Follows XDG Base Directory specification for cross-project shared datasets
 *
 * Path resolution:
 * - Uses XDG_DATA_HOME if set (e.g., /custom/path/trading25/datasets/prime.db)
 * - Otherwise uses $HOME/.local/share/trading25/datasets/
 * - Supports subdirectories (e.g., "markets/prime.db")
 * - Applies security validation (no absolute paths, no .., character validation)
 *
 * The directory hierarchy is automatically created if it doesn't exist.
 *
 * @param filename - Dataset filename or relative path (e.g., "prime.db", "markets/prime.db")
 * @returns Absolute path to dataset file
 */
export function getDatasetPath(filename: string): string {
  // Validate input and security constraints
  validateDatasetFilename(filename);

  // Remove leading ./ if present
  const normalizedFilename = filename.startsWith('./') ? filename.slice(2) : filename;

  // Get XDG base directory
  const dataHome = process.env.XDG_DATA_HOME || path.join(os.homedir(), '.local', 'share');
  const datasetsBaseDir = path.join(dataHome, 'trading25', 'datasets');

  // Construct full path with .db extension
  const fullPath = path.join(datasetsBaseDir, normalizedFilename);
  const finalPath = ensureDbExtension(fullPath);

  // Verify path bounds
  verifyPathBounds(finalPath, datasetsBaseDir, filename);

  // Create parent directory if it doesn't exist
  const parentDir = path.dirname(finalPath);
  if (!fs.existsSync(parentDir)) {
    try {
      fs.mkdirSync(parentDir, { recursive: true });
    } catch (error) {
      throw new Error(
        `Failed to create datasets directory at ${parentDir}: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  return finalPath;
}

/**
 * @deprecated Use getDatasetPath instead. Will be removed in v3.0.
 */
export const getDatasetV2Path = getDatasetPath;
