/**
 * Backtest Run Command
 *
 * 戦略バックテストを実行
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';

import { CLI_NAME } from '../../utils/constants.js';
import { CLIError, CLIValidationError } from '../../utils/error-handling.js';
import { handleBacktestError } from './error-handler.js';

export const runCommand = define({
  name: 'run',
  description: 'Run a backtest for a strategy',
  args: {
    strategy: {
      type: 'positional',
      description: 'Strategy name (e.g., range_break_v5, production/range_break_v5)',
      required: true,
    },
    wait: {
      type: 'boolean',
      short: 'w',
      description: 'Wait for completion (default: true)',
      default: true,
    },
    debug: {
      type: 'boolean',
      short: 'd',
      description: 'Enable debug output',
      default: false,
    },
    btUrl: {
      type: 'string',
      description: 'Backtest API server URL',
      default: process.env.BT_API_URL ?? 'http://localhost:3002',
    },
  },
  examples: `# Run a backtest
${CLI_NAME} backtest run range_break_v5

# Run with specific category
${CLI_NAME} backtest run production/range_break_v5

# Run without waiting
${CLI_NAME} backtest run range_break_v5 --no-wait`,
  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: CLI command with health check, run modes, and result display
  run: async (ctx) => {
    const { strategy, wait, debug, btUrl } = ctx.values;

    if (!strategy) {
      throw new CLIValidationError('strategy name is required');
    }

    // Dynamic import to avoid circular dependencies
    const { BacktestClient } = await import('@trading25/shared/clients/backtest');

    const client = new BacktestClient({ baseUrl: btUrl });

    // Check server health
    const spinner = ora('Connecting to backtest server...').start();

    try {
      await client.healthCheck();
      spinner.succeed('Connected to backtest server');
    } catch (error) {
      spinner.fail('Failed to connect to backtest server');
      handleBacktestError(ctx, error);
      throw new CLIError('Failed to connect to backtest server', 1, true, { cause: error });
    }

    // Run backtest
    ctx.log(chalk.cyan(`\nStarting backtest: ${chalk.bold(strategy)}`));

    const runSpinner = ora('Submitting backtest job...').start();

    try {
      if (wait) {
        // Wait for completion
        const job = await client.runAndWait(
          { strategy_name: strategy },
          {
            pollInterval: 2000,
            onProgress: (j) => {
              const progress = j.progress != null ? `${(j.progress * 100).toFixed(0)}%` : '';
              const message = j.message ?? 'Processing...';
              runSpinner.text = `${message} ${progress}`;
            },
          }
        );

        if (job.status === 'completed') {
          runSpinner.succeed('Backtest completed!');

          ctx.log('');
          ctx.log(chalk.green('=== Results ==='));
          ctx.log(`Job ID: ${chalk.cyan(job.job_id)}`);

          if (job.result) {
            ctx.log(`Total Return: ${formatPercent(job.result.total_return)}`);
            ctx.log(`Sharpe Ratio: ${chalk.yellow(job.result.sharpe_ratio.toFixed(2))}`);
            ctx.log(`Calmar Ratio: ${chalk.yellow(job.result.calmar_ratio.toFixed(2))}`);
            ctx.log(`Max Drawdown: ${formatPercent(job.result.max_drawdown, true)}`);
            ctx.log(`Win Rate: ${formatPercent(job.result.win_rate)}`);
            ctx.log(`Trade Count: ${chalk.cyan(job.result.trade_count)}`);
            if (job.result.html_path) {
              ctx.log(`HTML Report: ${chalk.gray(job.result.html_path)}`);
            }
          }
        } else if (job.status === 'cancelled') {
          runSpinner.warn('Backtest was cancelled');
        } else {
          runSpinner.fail(`Backtest failed: ${job.error ?? 'Unknown error'}`);
          throw new CLIError(`Backtest failed: ${job.error ?? 'Unknown error'}`, 1, true);
        }
      } else {
        // Don't wait, just submit
        const job = await client.runBacktest({ strategy_name: strategy });
        runSpinner.succeed(`Backtest submitted: ${job.job_id}`);
        ctx.log(chalk.gray(`Check status: ${CLI_NAME} backtest results ${job.job_id}`));
      }
    } catch (error) {
      if (error instanceof CLIError) {
        runSpinner.stop();
        throw error;
      }
      runSpinner.fail('Backtest failed');
      handleBacktestError(ctx, error);
      if (debug) {
        console.error(error);
      }
      throw new CLIError('Backtest failed', 1, true, { cause: error });
    }
  },
});

function formatPercent(value: number, negative = false): string {
  const formatted = `${(value * 100).toFixed(2)}%`;
  if (negative) {
    return value < 0 ? chalk.red(formatted) : chalk.green(formatted);
  }
  return value >= 0 ? chalk.green(formatted) : chalk.red(formatted);
}
