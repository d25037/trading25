/**
 * Error Handling Utilities
 * Common error handling patterns for CLI commands
 */

import chalk from 'chalk';
import type ora from 'ora';

/**
 * Base error class for CLI operations
 * Provides structured error handling with exit codes
 */
export class CLIError extends Error {
  constructor(
    message: string,
    public readonly exitCode: number = 1,
    public readonly silent: boolean = false,
    options?: { cause?: unknown }
  ) {
    super(message, options);
    this.name = 'CLIError';
  }
}

/**
 * Error thrown when input validation fails
 */
export class CLIValidationError extends CLIError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, 1, false, options);
    this.name = 'CLIValidationError';
  }
}

/**
 * Error thrown when user cancels an operation (e.g., confirmation prompt)
 * Treated as silent exit with code 0
 */
export class CLICancelError extends CLIError {
  constructor(message = 'Operation cancelled') {
    super(message, 0, true);
    this.name = 'CLICancelError';
  }
}

/**
 * Error thrown when a required resource is not found
 */
export class CLINotFoundError extends CLIError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, 1, false, options);
    this.name = 'CLINotFoundError';
  }
}

/**
 * Error thrown when an API request fails
 */
export class CLIAPIError extends CLIError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, 1, false, options);
    this.name = 'CLIAPIError';
  }
}

/**
 * Default troubleshooting tips for API-related errors
 */
const DEFAULT_TROUBLESHOOTING_TIPS = [
  'Ensure the API server is running: uv run bt server --port 3002',
  'Try with --debug flag for more information',
];

/**
 * Display troubleshooting tips
 */
export function displayTroubleshootingTips(tips: string[]): void {
  console.error(chalk.gray('\n💡 Troubleshooting tips:'));
  for (const tip of tips) {
    console.error(chalk.gray(`   • ${tip}`));
  }
}

/**
 * Handle command error with consistent formatting
 */
export function handleCommandError(
  error: unknown,
  spinner: ReturnType<typeof ora>,
  options: {
    failMessage: string;
    debug?: boolean;
    tips?: string[];
  }
): never {
  // Re-throw CLIError directly to preserve original exitCode/silent flags
  if (error instanceof CLIError) {
    spinner.stop();
    throw error;
  }

  spinner.fail(options.failMessage);

  const errorMessage = error instanceof Error ? error.message : String(error);
  console.error(chalk.red(`\nError: ${errorMessage}`));

  if (options.debug && error instanceof Error && error.stack) {
    console.error(chalk.gray(`\n[DEBUG] Stack trace:\n${error.stack}`));
  }

  displayTroubleshootingTips(options.tips ?? DEFAULT_TROUBLESHOOTING_TIPS);

  throw new CLIError(errorMessage, 1, true, { cause: error });
}

/**
 * Database command troubleshooting tips
 */
export const DB_TIPS = {
  refresh: [
    'Ensure the API server is running: uv run bt server --port 3002',
    'Ensure market.db exists: bun cli db sync',
    'Try with --debug flag for more information',
  ],
  sync: [
    'Ensure the API server is running: uv run bt server --port 3002',
    'Check if another sync is already running',
    'Try with --debug flag for more information',
  ],
};

/**
 * Dataset-specific troubleshooting tips
 */
export const DATASET_TIPS = {
  info: [
    'Ensure the API server is running: uv run bt server --port 3002',
    'Verify the dataset name is correct',
    'Try with --debug flag for more information',
  ],
  create: [
    'Ensure the API server is running: uv run bt server --port 3002',
    'Check if another dataset creation job is already running',
    'Use --overwrite to replace an existing dataset',
    'Try with --debug flag for more information',
  ],
  createTimeout: [
    'Use --timeout <minutes> to increase the timeout',
    'Check network connectivity',
    'Try a smaller preset (e.g., quickTesting)',
  ],
  search: [
    'Ensure the API server is running: uv run bt server --port 3002',
    'Verify the dataset name is correct',
    'Try with --debug flag for more information',
  ],
  sample: [
    'Ensure the API server is running: uv run bt server --port 3002',
    'Verify the dataset name is correct',
    'Try with --debug flag for more information',
  ],
};
