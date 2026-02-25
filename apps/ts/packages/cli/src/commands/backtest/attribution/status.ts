import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';

import { CLI_NAME } from '../../../utils/constants.js';
import { CLIError, CLIValidationError } from '../../../utils/error-handling.js';
import { handleBacktestError } from '../error-handler.js';
import { parseTableJsonFormat } from './format.js';

export const statusCommand = define({
  name: 'status',
  description: 'Get signal attribution job status',
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
    format: {
      type: 'string',
      short: 'f',
      description: 'Output format: table, json',
      default: 'table',
    },
    debug: {
      type: 'boolean',
      short: 'd',
      description: 'Enable debug output',
      default: false,
    },
  },
  examples: `# Get attribution job status
${CLI_NAME} backtest attribution status <job-id>

# Show status as JSON
${CLI_NAME} backtest attribution status <job-id> --format json`,
  run: async (ctx) => {
    const jobId = ctx.values.jobId;
    const btUrl = ctx.values['bt-url'];
    const format = parseTableJsonFormat(ctx.values.format);
    const debug = ctx.values.debug;
    if (!jobId) {
      throw new CLIValidationError('job ID is required');
    }

    const { BacktestClient } = await import('@trading25/api-clients/backtest');
    const client = new BacktestClient({ baseUrl: btUrl });
    const spinner = ora(`Fetching attribution job: ${jobId}`).start();

    try {
      const job = await client.getSignalAttributionJob(jobId);
      spinner.stop();

      if (format === 'json') {
        ctx.log(JSON.stringify(job, null, 2));
        return;
      }

      ctx.log('');
      ctx.log(chalk.bold('Attribution Job Status'));
      ctx.log(`  Job ID:     ${job.job_id}`);
      ctx.log(`  Status:     ${job.status}`);
      if (job.progress != null) {
        ctx.log(`  Progress:   ${(job.progress * 100).toFixed(0)}%`);
      }
      if (job.message) {
        ctx.log(`  Message:    ${job.message}`);
      }
      if (job.error) {
        ctx.log(chalk.red(`  Error:      ${job.error}`));
      }
      if (job.status === 'completed') {
        ctx.log(chalk.dim(`  See results: ${CLI_NAME} backtest attribution results ${job.job_id}`));
      }
    } catch (error) {
      spinner.fail('Failed to fetch attribution status');
      handleBacktestError(ctx, error);
      if (debug) {
        console.error(error);
      }
      throw new CLIError('Failed to fetch attribution status', 1, true, { cause: error });
    }
  },
});
