/**
 * Dataset - Create Command
 * Dataset creation via API with job polling
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';
import { ApiClient, type DatasetJobResponse, type DatasetPreset } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import {
  displayError,
  displayFooter,
  displayHeader,
  displayKeyValue,
  displayList,
  displaySuccess,
  displayWarning,
} from '../../utils/display-helpers.js';
import {
  CLIError,
  CLIValidationError,
  DATASET_TIPS,
  displayTroubleshootingTips,
  handleCommandError,
} from '../../utils/error-handling.js';
import { formatElapsedTime, logDebug, sleep } from '../../utils/format-helpers.js';

/**
 * Polling interval in milliseconds
 */
const POLL_INTERVAL = 2000;

/**
 * Default timeout in minutes
 */
const DEFAULT_TIMEOUT_MINUTES = 30;

/**
 * Valid preset names
 */
const VALID_PRESETS = [
  'fullMarket',
  'primeMarket',
  'standardMarket',
  'growthMarket',
  'quickTesting',
  'topix100',
  'topix500',
  'mid400',
  'primeExTopix500',
] as const;

/**
 * Normalize preset name (handle case variations)
 */
function normalizePreset(preset: string): DatasetPreset {
  const normalizedMap: Record<string, DatasetPreset> = {
    quick: 'quickTesting',
    quicktesting: 'quickTesting',
    fullmarket: 'fullMarket',
    primemarket: 'primeMarket',
    standardmarket: 'standardMarket',
    growthmarket: 'growthMarket',
    primeextopix500: 'primeExTopix500',
  };

  const lowercasePreset = preset.toLowerCase();
  if (normalizedMap[lowercasePreset]) {
    return normalizedMap[lowercasePreset];
  }

  if (VALID_PRESETS.includes(preset as DatasetPreset)) {
    return preset as DatasetPreset;
  }

  throw new Error(`Unknown preset: ${preset}. Available: ${VALID_PRESETS.join(', ')}`);
}

/**
 * Error thrown when job polling times out
 */
class JobTimeoutError extends Error {
  constructor(
    public readonly jobId: string,
    public readonly timeoutMinutes: number
  ) {
    super(`Job timed out after ${timeoutMinutes} minutes`);
    this.name = 'JobTimeoutError';
  }
}

/**
 * State for tracking timeout warnings
 */
interface WarningState {
  warned75: boolean;
  warned90: boolean;
}

/**
 * Check and emit timeout warnings at 75% and 90% thresholds
 */
function checkTimeoutWarnings(elapsed: number, timeoutMs: number, state: WarningState): void {
  const threshold75 = timeoutMs * 0.75;
  const threshold90 = timeoutMs * 0.9;

  if (!state.warned75 && elapsed > threshold75) {
    console.log(chalk.yellow(`\n⚠ Warning: 75% of timeout reached (${formatElapsedTime(elapsed)})`));
    state.warned75 = true;
  }
  if (!state.warned90 && elapsed > threshold90) {
    console.log(chalk.yellow(`\n⚠ Warning: 90% of timeout reached (${formatElapsedTime(elapsed)})`));
    state.warned90 = true;
  }
}

async function pollJobStatus(
  apiClient: ApiClient,
  jobId: string,
  spinner: ReturnType<typeof ora>,
  debug: boolean,
  timeoutMinutes: number
): Promise<DatasetJobResponse> {
  const startTime = Date.now();
  const timeoutMs = timeoutMinutes * 60 * 1000;
  const warningState: WarningState = { warned75: false, warned90: false };

  while (true) {
    const elapsed = Date.now() - startTime;

    if (elapsed > timeoutMs) {
      throw new JobTimeoutError(jobId, timeoutMinutes);
    }

    checkTimeoutWarnings(elapsed, timeoutMs, warningState);

    const job = await apiClient.getDatasetJobStatus(jobId);

    if (debug) {
      console.log(chalk.gray(`[DEBUG] Job status: ${job.status} (elapsed: ${formatElapsedTime(elapsed)})`));
    }

    if (job.progress) {
      const { stage, percentage, message } = job.progress;
      spinner.text = `[${stage}] ${message} (${percentage.toFixed(1)}%) - ${formatElapsedTime(elapsed)}`;
    }

    if (job.status === 'completed' || job.status === 'failed' || job.status === 'cancelled') {
      return job;
    }

    await sleep(POLL_INTERVAL);
  }
}

function displayCreationResult(result: NonNullable<DatasetJobResponse['result']>): void {
  displayHeader('Dataset Creation Summary');

  displayKeyValue('Status', result.success ? chalk.green('✓ Success') : chalk.red('✗ Failed'), 0);
  displayKeyValue('Total Stocks', chalk.yellow(result.totalStocks.toString()), 0);
  displayKeyValue('Processed', chalk.yellow(result.processedStocks.toString()), 0);
  displayKeyValue('Output', chalk.cyan(result.outputPath), 0);

  if (result.warnings.length > 0) {
    console.log(chalk.yellow(`\nWarnings (${result.warnings.length}):`));
    displayList(result.warnings, { color: 'yellow' });
  }

  if (result.errors.length > 0) {
    console.log(chalk.red(`\nErrors (${result.errors.length}):`));
    displayList(result.errors, { color: 'red' });
  }

  displayFooter();
}

function handleJobResult(job: DatasetJobResponse, spinner: ReturnType<typeof ora>): void {
  spinner.stop();

  if (job.status === 'completed' && job.result) {
    displayCreationResult(job.result);

    if (job.result.success) {
      displaySuccess(`Dataset created successfully: ${job.name}`);
      return;
    }

    displayError('Dataset creation completed with errors');
    throw new CLIError('Dataset creation completed with errors', 1, true);
  }

  if (job.status === 'failed') {
    console.error(chalk.red(`\n❌ Dataset creation failed: ${job.error || 'Unknown error'}`));
    throw new CLIError(`Dataset creation failed: ${job.error || 'Unknown error'}`, 1, true);
  }

  if (job.status === 'cancelled') {
    displayWarning('Dataset creation was cancelled');
    throw new CLIError('Dataset creation was cancelled', 1, true);
  }
}

async function handleTimeoutError(
  error: JobTimeoutError,
  apiClient: ApiClient,
  spinner: ReturnType<typeof ora>
): Promise<never> {
  spinner.fail('Dataset creation timed out');

  console.error(chalk.red(`\nError: ${error.message}`));

  // Try to cancel the job
  try {
    console.log(chalk.gray('Attempting to cancel the job...'));
    await apiClient.cancelDatasetJob(error.jobId);
    displayWarning('Job cancelled successfully');
  } catch {
    console.log(chalk.gray('Could not cancel the job (it may have already completed or failed)'));
  }

  displayTroubleshootingTips(DATASET_TIPS.createTimeout);

  throw new CLIError(error.message, 1, true, { cause: error });
}

function handleCreateError(error: unknown, spinner: ReturnType<typeof ora>, debug: boolean): never {
  handleCommandError(error, spinner, {
    failMessage: 'Dataset creation failed',
    debug,
    tips: DATASET_TIPS.create,
  });
}

/**
 * Unified error handler for dataset creation
 * Dispatches to appropriate handler based on error type
 */
async function handleError(
  error: unknown,
  apiClient: ApiClient,
  spinner: ReturnType<typeof ora>,
  isDebug: boolean
): Promise<never> {
  if (error instanceof CLIError) {
    spinner.stop();
    throw error;
  }
  if (error instanceof JobTimeoutError) {
    return handleTimeoutError(error, apiClient, spinner);
  }
  return handleCreateError(error, spinner, isDebug);
}

/**
 * Validate command options and exit if invalid
 */
function validateOptions(
  output: string | undefined,
  isResume: boolean,
  overwrite: boolean | undefined
): asserts output is string {
  if (!output) {
    throw new CLIValidationError('Output filename is required');
  }

  if (isResume && overwrite) {
    throw new CLIValidationError('Cannot use --resume and --overwrite together');
  }
}

/**
 * Create command definition
 */
export const createCommand = define({
  name: 'create',
  description: 'Create a new dataset snapshot using a preset configuration',
  args: {
    output: {
      type: 'positional',
      description: 'Output filename (within XDG datasets directory)',
    },
    preset: {
      type: 'string',
      description: 'Preset configuration name',
      default: 'quickTesting',
    },
    overwrite: {
      type: 'boolean',
      description: 'Overwrite existing dataset',
    },
    resume: {
      type: 'boolean',
      description: 'Resume fetching missing data for existing dataset',
    },
    timeout: {
      type: 'number',
      description: 'Maximum time to wait in minutes (default: 30)',
      default: DEFAULT_TIMEOUT_MINUTES,
    },
    debug: {
      type: 'boolean',
      description: 'Enable detailed output for debugging',
    },
  },
  examples: `
PRESETS:
  [Basic]
    fullMarket            All markets (Prime+Standard+Growth), 10 years
    primeMarket           Prime market only (market cap >= 100B yen), 10 years
    standardMarket        Standard market only, 10 years
    growthMarket          Growth market only, 10 years

  [Testing]
    quickTesting          CI/development (Prime, 3 stocks), 10 years [default]

  [Index-based]
    topix100              TOPIX 100 (Core30 + Large70), 10 years
    topix500              TOPIX 500 (Core30 + Large70 + Mid400), 10 years
    mid400                TOPIX Mid400 only, 10 years
    primeExTopix500       Prime excluding TOPIX 500, 10 years

EXAMPLES:
  # Create with default preset (quickTesting)
  ${CLI_NAME} dataset create test.db

  # Create prime market dataset
  ${CLI_NAME} dataset create prime.db --preset primeMarket

  # Create TOPIX 100 dataset
  ${CLI_NAME} dataset create topix100.db --preset topix100

  # Create with overwrite
  ${CLI_NAME} dataset create prime.db --preset primeMarket --overwrite

  # Resume fetching missing data for existing dataset
  ${CLI_NAME} dataset create primeExTopix500.db --preset primeExTopix500 --resume
  `.trim(),
  run: async (ctx) => {
    const { output, preset, overwrite, resume, timeout, debug } = ctx.values;
    const isResume = resume ?? false;
    const isDebug = debug ?? false;
    const timeoutMinutes = timeout ?? DEFAULT_TIMEOUT_MINUTES;

    // Validate before starting spinner to avoid spinner leak on validation error
    validateOptions(output, isResume, overwrite);
    const normalizedPreset = normalizePreset(preset ?? 'testing');

    logDebug(isDebug, `Output: ${output}, Preset: ${preset}, Overwrite: ${overwrite ?? false}`);

    const spinner = ora(isResume ? 'Initializing dataset resume...' : 'Initializing dataset creation...').start();
    const apiClient = new ApiClient();

    logDebug(isDebug, `Normalized preset: ${normalizedPreset}, Timeout: ${timeoutMinutes}m, Resume: ${isResume}`);

    try {
      spinner.text = isResume ? 'Starting dataset resume job...' : 'Starting dataset creation job...';
      const createResponse = isResume
        ? await apiClient.startDatasetResume(output, normalizedPreset)
        : await apiClient.startDatasetCreate(output, normalizedPreset, overwrite ?? false);

      logDebug(isDebug, `Job ID: ${createResponse.jobId}, Estimated: ${createResponse.estimatedTime || 'unknown'}`);

      const actionText = isResume ? 'resume' : 'creation';
      spinner.text = `Dataset ${actionText} started (estimated: ${createResponse.estimatedTime || 'unknown'}, timeout: ${timeoutMinutes}m)...`;

      const job = await pollJobStatus(apiClient, createResponse.jobId, spinner, isDebug, timeoutMinutes);
      handleJobResult(job, spinner);
    } catch (error) {
      await handleError(error, apiClient, spinner, isDebug);
    }
  },
});

// Export handler for potential standalone use
export { normalizePreset };
