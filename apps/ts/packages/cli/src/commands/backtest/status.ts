/**
 * Backtest Status Command
 *
 * bt サーバーステータス確認コマンド
 */

import { BacktestClient } from '@trading25/api-clients/backtest';
import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';
import { CLI_NAME } from '../../utils/constants.js';
import { CLIError } from '../../utils/error-handling.js';
import { handleBacktestError } from './error-handler.js';

export const statusCommand = define({
  name: 'status',
  description: 'Check bt server status',
  args: {
    btUrl: {
      type: 'string',
      description: 'Backtest API server URL',
      default: process.env.BT_API_URL ?? 'http://localhost:3002',
    },
    debug: {
      type: 'boolean',
      description: 'Enable debug output',
    },
  },
  examples: `# Check bt server status
${CLI_NAME} backtest status

# Check with custom URL
${CLI_NAME} backtest status --bt-url http://localhost:3002`,
  run: async (ctx) => {
    const { btUrl, debug } = ctx.values;

    const client = new BacktestClient({ baseUrl: btUrl });
    const spinner = ora();

    try {
      spinner.start(`Checking bt server at ${btUrl}...`);
      const health = await client.healthCheck();
      spinner.succeed('bt server is healthy');

      ctx.log('');
      ctx.log(chalk.bold('Server Status'));
      ctx.log(`  Service:  ${chalk.cyan(health.service)}`);
      ctx.log(`  Version:  ${chalk.cyan(health.version)}`);
      ctx.log(`  Status:   ${chalk.green(health.status)}`);
      ctx.log(`  URL:      ${chalk.dim(btUrl)}`);
    } catch (error) {
      spinner.fail('bt server is not available');
      handleBacktestError(ctx, error);
      if (debug && error instanceof Error && error.stack) {
        ctx.log(chalk.dim(error.stack));
      }
      throw new CLIError('bt server is not available', 1, true, { cause: error });
    }
  },
});
