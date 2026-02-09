/**
 * Backtest Cancel Command
 *
 * 実行中のバックテストジョブをキャンセル
 */

import { BacktestApiError } from '@trading25/clients-ts/backtest';
import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';

import { CLI_NAME } from '../../utils/constants.js';
import { CLIValidationError, handleCommandError } from '../../utils/error-handling.js';

export const cancelCommand = define({
  name: 'cancel',
  description: 'Cancel a running backtest job',
  args: {
    jobId: {
      type: 'positional',
      description: 'Job ID to cancel',
      required: true,
    },
    btUrl: {
      type: 'string',
      description: 'Backtest API server URL',
      default: process.env.BT_API_URL ?? 'http://localhost:3002',
    },
    debug: {
      type: 'boolean',
      short: 'd',
      description: 'Enable debug output',
      default: false,
    },
  },
  examples: `# Cancel a running backtest
${CLI_NAME} backtest cancel <job-id>`,
  run: async (ctx) => {
    const { jobId, btUrl, debug } = ctx.values;

    if (!jobId) {
      throw new CLIValidationError('job ID is required');
    }

    const { BacktestClient } = await import('@trading25/clients-ts/backtest');
    const client = new BacktestClient({ baseUrl: btUrl });
    const spinner = ora('Cancelling backtest job...').start();

    try {
      const job = await client.cancelJob(jobId);
      spinner.succeed('Backtest job cancelled');
      ctx.log(`Job ID: ${chalk.cyan(job.job_id)}`);
      ctx.log(`Status: ${chalk.yellow(job.status)}`);
    } catch (error) {
      if (error instanceof BacktestApiError && error.status === 409) {
        spinner.warn('Job cannot be cancelled (already completed or failed)');
        return;
      }
      handleCommandError(error, spinner, {
        failMessage: 'Failed to cancel backtest job',
        debug,
        tips: ['Ensure the bt server is running: uv run bt server', 'Try with --debug flag for more information'],
      });
    }
  },
});
