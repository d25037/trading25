import { BacktestApiError } from '@trading25/api-clients/backtest';
import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';

import { CLI_NAME } from '../../../utils/constants.js';
import { CLIValidationError, handleCommandError } from '../../../utils/error-handling.js';

export const cancelCommand = define({
  name: 'cancel',
  description: 'Cancel a running attribution job',
  args: {
    jobId: {
      type: 'positional',
      description: 'Attribution job ID',
      required: true,
    },
    'bt-url': {
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
  examples: `# Cancel attribution job
${CLI_NAME} backtest attribution cancel <job-id>`,
  run: async (ctx) => {
    const jobId = ctx.values.jobId;
    const btUrl = ctx.values['bt-url'];
    const debug = ctx.values.debug;
    if (!jobId) {
      throw new CLIValidationError('job ID is required');
    }

    const { BacktestClient } = await import('@trading25/api-clients/backtest');
    const client = new BacktestClient({ baseUrl: btUrl });
    const spinner = ora('Cancelling attribution job...').start();

    try {
      const job = await client.cancelSignalAttributionJob(jobId);
      spinner.succeed('Attribution job cancelled');
      ctx.log(`Job ID: ${chalk.cyan(job.job_id)}`);
      ctx.log(`Status: ${chalk.yellow(job.status)}`);
    } catch (error) {
      if (error instanceof BacktestApiError && error.status === 409) {
        spinner.warn('Attribution job cannot be cancelled (already completed or failed)');
        return;
      }
      handleCommandError(error, spinner, {
        failMessage: 'Failed to cancel attribution job',
        debug,
        tips: ['Ensure the bt server is running: uv run bt server', 'Try with --debug flag for more information'],
      });
    }
  },
});
