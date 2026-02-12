import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';

import type { SignalAttributionJobResponse } from '@trading25/clients-ts/backtest';
import { CLI_NAME } from '../../../utils/constants.js';
import { CLIError, CLIValidationError } from '../../../utils/error-handling.js';
import { handleBacktestError } from '../error-handler.js';
import { parseTableJsonFormat } from './format.js';

function parsePositiveInt(value: string, name: string): number {
  if (!/^\d+$/.test(value.trim())) {
    throw new CLIValidationError(`${name} must be a positive integer`);
  }

  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1) {
    throw new CLIValidationError(`${name} must be a positive integer`);
  }
  return parsed;
}

function parseOptionalInt(value: string | undefined): number | null {
  if (!value || value.trim().length === 0) return null;
  if (!/^-?\d+$/.test(value.trim())) {
    throw new CLIValidationError('randomSeed must be an integer');
  }

  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) {
    throw new CLIValidationError('randomSeed must be an integer');
  }
  return parsed;
}

function formatRate(value: number): string {
  const formatted = `${(value * 100).toFixed(2)}%`;
  return value >= 0 ? chalk.green(`+${formatted}`) : chalk.red(formatted);
}

function formatSigned(value: number): string {
  const sign = value >= 0 ? '+' : '';
  return `${sign}${value.toFixed(4)}`;
}

function printCompletedSummary(ctx: { log: (msg: string) => void }, job: SignalAttributionJobResponse): void {
  const result = job.result_data;
  if (!result) {
    ctx.log(chalk.yellow('Result data is empty'));
    return;
  }

  ctx.log('');
  ctx.log(chalk.green('=== Signal Attribution Results ==='));
  ctx.log(`Job ID: ${chalk.cyan(job.job_id)}`);
  ctx.log(`Baseline Return: ${formatRate(result.baseline_metrics.total_return)}`);
  ctx.log(`Baseline Sharpe: ${chalk.yellow(formatSigned(result.baseline_metrics.sharpe_ratio))}`);
  ctx.log(
    `TopN: requested=${result.top_n_selection.top_n_requested}, effective=${result.top_n_selection.top_n_effective}`
  );
  ctx.log(`Shapley Method: ${result.shapley.method ?? '-'}`);
  if (result.top_n_selection.selected_signal_ids.length > 0) {
    ctx.log(`Shapley Signals: ${result.top_n_selection.selected_signal_ids.join(', ')}`);
  }
}

export const runCommand = define({
  name: 'run',
  description: 'Run signal attribution for a strategy',
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
    'shapley-top-n': {
      type: 'string',
      description: 'Top N signals for Shapley',
      default: '5',
    },
    'shapley-permutations': {
      type: 'string',
      description: 'Permutation samples for approximate Shapley',
      default: '128',
    },
    'random-seed': {
      type: 'string',
      description: 'Optional random seed for Shapley approximation',
    },
    format: {
      type: 'string',
      short: 'f',
      description: 'Output format: table, json',
      default: 'table',
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
  examples: `# Run attribution and wait
${CLI_NAME} backtest attribution run range_break_v5

# Run attribution without waiting
${CLI_NAME} backtest attribution run range_break_v5 --no-wait

  # Run attribution with advanced parameters
${CLI_NAME} backtest attribution run range_break_v5 --shapley-top-n 8 --shapley-permutations 256 --random-seed 42`,
  run: async (ctx) => {
    const strategy = ctx.values.strategy;
    const wait = ctx.values.wait;
    const shapleyTopN = ctx.values['shapley-top-n'];
    const shapleyPermutations = ctx.values['shapley-permutations'];
    const randomSeed = ctx.values['random-seed'];
    const format = parseTableJsonFormat(ctx.values.format);
    const btUrl = ctx.values['bt-url'];
    const debug = ctx.values.debug;
    if (!strategy) {
      throw new CLIValidationError('strategy name is required');
    }

    const topN = parsePositiveInt(shapleyTopN, 'shapleyTopN');
    const permutations = parsePositiveInt(shapleyPermutations, 'shapleyPermutations');
    const seed = parseOptionalInt(randomSeed);

    const { BacktestClient } = await import('@trading25/clients-ts/backtest');
    const client = new BacktestClient({ baseUrl: btUrl });

    const spinner = ora('Submitting signal attribution job...').start();
    try {
      if (wait) {
        const job = await client.runSignalAttributionAndWait(
          {
            strategy_name: strategy,
            shapley_top_n: topN,
            shapley_permutations: permutations,
            random_seed: seed,
          },
          {
            pollInterval: 2000,
            onProgress: (j) => {
              const progress = j.progress != null ? `${(j.progress * 100).toFixed(0)}%` : '';
              const message = j.message ?? 'Processing...';
              spinner.text = `${message} ${progress}`;
            },
          }
        );

        if (job.status === 'completed') {
          spinner.succeed('Signal attribution completed');
          if (format === 'json') {
            ctx.log(JSON.stringify(job, null, 2));
            return;
          }
          printCompletedSummary(ctx, job);
          return;
        }

        if (job.status === 'cancelled') {
          spinner.warn('Signal attribution was cancelled');
          return;
        }

        spinner.fail(`Signal attribution failed: ${job.error ?? 'Unknown error'}`);
        throw new CLIError(`Signal attribution failed: ${job.error ?? 'Unknown error'}`, 1, true);
      }

      const job = await client.runSignalAttribution({
        strategy_name: strategy,
        shapley_top_n: topN,
        shapley_permutations: permutations,
        random_seed: seed,
      });
      spinner.succeed(`Signal attribution submitted: ${job.job_id}`);
      if (format === 'json') {
        ctx.log(JSON.stringify(job, null, 2));
      } else {
        ctx.log(chalk.dim(`Check status: ${CLI_NAME} backtest attribution status ${job.job_id}`));
      }
    } catch (error) {
      if (error instanceof CLIError) {
        spinner.stop();
        throw error;
      }
      spinner.fail('Signal attribution failed');
      handleBacktestError(ctx, error);
      if (debug) {
        console.error(error);
      }
      throw new CLIError('Signal attribution failed', 1, true, { cause: error });
    }
  },
});
