import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';

import type { SignalAttributionResult } from '@trading25/api-clients/backtest';
import { CLI_NAME } from '../../../utils/constants.js';
import { CLIError, CLIValidationError } from '../../../utils/error-handling.js';
import { handleBacktestError } from '../error-handler.js';
import { parseTableJsonFormat } from './format.js';

function formatRate(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  const formatted = `${(value * 100).toFixed(2)}%`;
  return value >= 0 ? chalk.green(`+${formatted}`) : chalk.red(formatted);
}

function formatSigned(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  const sign = value >= 0 ? '+' : '';
  return `${sign}${value.toFixed(4)}`;
}

function printResult(ctx: { log: (msg: string) => void }, result: SignalAttributionResult): void {
  ctx.log('');
  ctx.log(chalk.bold('Signal Attribution Result'));
  ctx.log(`Baseline Return: ${formatRate(result.baseline_metrics.total_return)}`);
  ctx.log(`Baseline Sharpe: ${chalk.yellow(formatSigned(result.baseline_metrics.sharpe_ratio))}`);
  ctx.log(
    `TopN Selection: requested=${result.top_n_selection.top_n_requested}, effective=${result.top_n_selection.top_n_effective}`
  );
  ctx.log(`Shapley Method: ${result.shapley.method ?? '-'}`);
  if (result.top_n_selection.selected_signal_ids.length > 0) {
    ctx.log(`Shapley Signals: ${result.top_n_selection.selected_signal_ids.join(', ')}`);
  }

  ctx.log('');
  ctx.log(chalk.bold('Signals:'));
  for (const signal of result.signals) {
    const shapleyStatus = signal.shapley?.status ?? '-';
    ctx.log(
      `  ${chalk.cyan(signal.signal_id)} | loo=${signal.loo.status} ret=${formatRate(signal.loo.delta_total_return)} sharpe=${formatSigned(signal.loo.delta_sharpe_ratio)} | shapley=${shapleyStatus} ret=${formatRate(signal.shapley?.total_return)} sharpe=${formatSigned(signal.shapley?.sharpe_ratio)}`
    );
  }
}

export const resultsCommand = define({
  name: 'results',
  description: 'Show signal attribution result for a completed job',
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
  examples: `# Show attribution result
${CLI_NAME} backtest attribution results <job-id>

# Show attribution result as JSON
${CLI_NAME} backtest attribution results <job-id> --format json`,
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
    const spinner = ora(`Fetching attribution result: ${jobId}`).start();

    try {
      const response = await client.getSignalAttributionResult(jobId);
      spinner.stop();
      if (format === 'json') {
        ctx.log(JSON.stringify(response, null, 2));
        return;
      }
      printResult(ctx, response.result);
    } catch (error) {
      spinner.fail('Failed to fetch attribution result');
      handleBacktestError(ctx, error);
      if (debug) {
        console.error(error);
      }
      throw new CLIError('Failed to fetch attribution result', 1, true, { cause: error });
    }
  },
});
