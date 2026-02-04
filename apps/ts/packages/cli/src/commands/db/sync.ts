/**
 * Market Sync Command
 * Synchronize market-wide data with incremental updates via API
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';
import { ApiClient, type SyncJobResponse, type SyncMode } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { CLIError, DB_TIPS, handleCommandError } from '../../utils/error-handling.js';

/**
 * Polling interval in milliseconds
 */
const POLL_INTERVAL = 2000;

/**
 * Display sync result summary
 */
function displaySyncResult(result: NonNullable<SyncJobResponse['result']>): void {
  console.log(`\n${chalk.bold('='.repeat(60))}`);
  console.log(chalk.bold.cyan('Market Sync Summary'));
  console.log(chalk.bold('='.repeat(60)));

  console.log(chalk.white(`Status: ${result.success ? chalk.green('✓ Success') : chalk.red('✗ Failed')}`));
  console.log(chalk.white(`API Calls: ${chalk.yellow(result.totalApiCalls.toString())}`));

  if (result.stocksUpdated > 0) {
    console.log(chalk.white(`Stocks Updated: ${chalk.yellow(result.stocksUpdated.toString())}`));
  }

  console.log(chalk.white(`Dates Processed: ${chalk.yellow(result.datesProcessed.toString())}`));

  if (result.failedDates.length > 0) {
    console.log(
      chalk.white(`Failed Dates: ${chalk.red(result.failedDates.length.toString())} (will retry on next sync)`)
    );
    const dateStrings = result.failedDates.join(', ');
    console.log(chalk.gray(`  ${dateStrings}`));
  }

  if (result.errors.length > 0) {
    console.log(chalk.red('\nErrors:'));
    for (const error of result.errors) {
      console.log(chalk.red(`  • ${error}`));
    }
  }

  console.log(`${chalk.bold('='.repeat(60))}\n`);
}

/**
 * Determine sync mode from options
 */
function determineSyncMode(init?: boolean, update?: boolean, initIndices?: boolean): SyncMode {
  if (init) {
    return 'initial';
  }
  if (initIndices) {
    return 'indices-only';
  }
  if (update) {
    return 'incremental';
  }
  return 'auto';
}

/**
 * Wait for specified milliseconds
 */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Poll for job completion
 */
async function pollJobStatus(
  apiClient: ApiClient,
  jobId: string,
  spinner: ReturnType<typeof ora>,
  debug: boolean
): Promise<SyncJobResponse> {
  while (true) {
    const job = await apiClient.getSyncJobStatus(jobId);

    if (debug) {
      console.log(chalk.gray(`[DEBUG] Job status: ${job.status}`));
    }

    // Update spinner with progress
    if (job.progress) {
      const { stage, percentage, message } = job.progress;
      spinner.text = `[${stage}] ${message} (${percentage.toFixed(1)}%)`;
    }

    // Check for completion states
    if (job.status === 'completed') {
      return job;
    }

    if (job.status === 'failed') {
      return job;
    }

    if (job.status === 'cancelled') {
      return job;
    }

    // Wait before next poll
    await sleep(POLL_INTERVAL);
  }
}

/**
 * Check if this was an initial sync based on mode and API calls
 */
function isInitialSync(mode: SyncMode, apiCalls: number): boolean {
  return mode === 'initial' || (mode === 'auto' && apiCalls > 10);
}

/**
 * Get sync type name for display
 */
function getSyncTypeName(mode: SyncMode, apiCalls: number): string {
  if (mode === 'indices-only') {
    return 'Indices initialization';
  }
  return isInitialSync(mode, apiCalls) ? 'Initial sync' : 'Incremental update';
}

/**
 * Handle completed job status
 */
function handleCompletedJob(result: NonNullable<SyncJobResponse['result']>, mode: SyncMode): void {
  displaySyncResult(result);

  const syncType = getSyncTypeName(mode, result.totalApiCalls);
  const isInitial = isInitialSync(mode, result.totalApiCalls) || mode === 'indices-only';

  if (result.success) {
    console.log(chalk.green(`✓ ${syncType} completed successfully`));
    return;
  }

  if (isInitial) {
    console.log(chalk.red(`✗ ${syncType} completed with errors`));
    throw new CLIError(`${syncType} completed with errors`, 1, true);
  }

  console.log(chalk.yellow(`⚠ ${syncType} completed with some errors`));
}

/**
 * Handle job result and exit accordingly
 */
function handleJobResult(job: SyncJobResponse, mode: SyncMode, spinner: ReturnType<typeof ora>): void {
  spinner.stop();

  if (job.status === 'completed' && job.result) {
    handleCompletedJob(job.result, mode);
    return;
  }

  if (job.status === 'failed') {
    console.error(chalk.red(`\n❌ Sync failed: ${job.error || 'Unknown error'}`));
    throw new CLIError(`Sync failed: ${job.error || 'Unknown error'}`, 1, true);
  }

  if (job.status === 'cancelled') {
    console.log(chalk.yellow('\n⚠ Sync job was cancelled'));
    throw new CLIError('Sync job was cancelled', 1, true);
  }
}

/**
 * Sync command definition
 */
export const syncCommand = define({
  name: 'sync',
  description: 'Synchronize market data (auto-detects initial vs incremental)',
  args: {
    init: {
      type: 'boolean',
      description: 'Force initial sync (2 years of data, ~552 API calls)',
    },
    'init-indices': {
      type: 'boolean',
      description: 'Initialize indices only (52 API calls, use when stocks synced but indices empty)',
    },
    update: {
      type: 'boolean',
      description: 'Force incremental update only',
    },
    debug: {
      type: 'boolean',
      description: 'Enable debug logging',
    },
  },
  examples: `
# Auto-detect sync strategy (includes auto indices init if empty)
${CLI_NAME} db sync

# Force initial sync (2 years of data, stocks + indices)
${CLI_NAME} db sync --init

# Initialize indices only (52 API calls)
${CLI_NAME} db sync --init-indices

# Force incremental update only
${CLI_NAME} db sync --update

# Enable debug logging
${CLI_NAME} db sync --debug
  `.trim(),
  run: async (ctx) => {
    const { init, 'init-indices': initIndices, update, debug } = ctx.values;
    const spinner = ora('Initializing market sync...').start();
    const mode = determineSyncMode(init, update, initIndices);

    if (debug) {
      console.log(chalk.gray(`[DEBUG] Using API endpoint for sync`));
      console.log(chalk.gray(`[DEBUG] Mode: ${mode}`));
    }

    try {
      const apiClient = new ApiClient();

      // Start sync job
      spinner.text = 'Starting sync job...';
      const createResponse = await apiClient.startSync(mode);

      if (debug) {
        console.log(chalk.gray(`[DEBUG] Job ID: ${createResponse.jobId}`));
        console.log(chalk.gray(`[DEBUG] Estimated API calls: ${createResponse.estimatedApiCalls}`));
      }

      spinner.text = `Sync job started (${createResponse.estimatedApiCalls} estimated API calls)...`;

      // Poll for completion
      const job = await pollJobStatus(apiClient, createResponse.jobId, spinner, debug ?? false);

      // Handle result
      handleJobResult(job, mode, spinner);
    } catch (error) {
      handleCommandError(error, spinner, {
        failMessage: 'Sync failed',
        debug,
        tips: DB_TIPS.sync,
      });
    }
  },
});
