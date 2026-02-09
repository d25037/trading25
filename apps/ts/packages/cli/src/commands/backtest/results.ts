/**
 * Backtest Results Command
 *
 * バックテスト結果表示コマンド
 */

import { BacktestClient, type BacktestJobResponse } from '@trading25/clients-ts/backtest';
import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';

import { CLI_NAME } from '../../utils/constants.js';
import { CLIError } from '../../utils/error-handling.js';
import { handleBacktestError } from './error-handler.js';

export const resultsCommand = define({
  name: 'results',
  description: 'Show backtest results',
  args: {
    jobId: {
      type: 'positional',
      description: 'Job ID (optional, shows list if omitted)',
    },
    limit: {
      type: 'string',
      short: 'l',
      description: 'Number of results to show',
      default: '10',
    },
    btUrl: {
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
      description: 'Enable debug output',
    },
  },
  examples: `# Show recent results
${CLI_NAME} backtest results

# Show specific job result
${CLI_NAME} backtest results <job-id>

# Show more results
${CLI_NAME} backtest results --limit 20

# Output as JSON
${CLI_NAME} backtest results --format json`,
  run: async (ctx) => {
    const { jobId, limit, btUrl, format, debug: _debug } = ctx.values;

    const client = new BacktestClient({ baseUrl: btUrl });
    const spinner = ora();

    try {
      if (jobId) {
        // 特定のジョブの詳細を表示
        spinner.start(`Fetching result: ${jobId}`);
        const job = await client.getJobStatus(jobId);
        spinner.stop();

        if (format === 'json') {
          ctx.log(JSON.stringify(job, null, 2));
          return;
        }

        displayJobDetail(ctx, job);
      } else {
        // ジョブ一覧を表示
        spinner.start('Fetching results...');
        const jobs = await client.listJobs(Number(limit) || 10);
        spinner.stop();

        if (format === 'json') {
          ctx.log(JSON.stringify(jobs, null, 2));
          return;
        }

        displayJobList(ctx, jobs);
      }
    } catch (error) {
      spinner.fail('Failed to fetch results');
      handleBacktestError(ctx, error);
      throw new CLIError('Failed to fetch results', 1, true, { cause: error });
    }
  },
});

function displayJobList(ctx: { log: (msg: string) => void }, jobs: BacktestJobResponse[]): void {
  if (jobs.length === 0) {
    ctx.log(chalk.yellow('No results found'));
    return;
  }

  ctx.log('');
  ctx.log(chalk.bold('Recent Backtest Jobs'));
  ctx.log('');

  for (const job of jobs) {
    const statusIcon = getStatusIcon(job.status);
    const createdAt = new Date(job.created_at).toLocaleString('ja-JP');
    const jobIdShort = job.job_id.substring(0, 8);

    let resultInfo = '';
    if (job.status === 'completed' && job.result) {
      const returnStr = formatPercent(job.result.total_return);
      resultInfo = ` → Return: ${returnStr}`;
    } else if (job.status === 'failed' && job.error) {
      resultInfo = ` → ${chalk.red(job.error.substring(0, 50))}`;
    }

    ctx.log(`${statusIcon} ${chalk.dim(jobIdShort)} ${chalk.dim(createdAt)}${resultInfo}`);
  }

  ctx.log('');
  ctx.log(chalk.dim(`Use '${CLI_NAME} backtest results <job-id>' to see details`));
}

function displayJobDetail(ctx: { log: (msg: string) => void }, job: BacktestJobResponse): void {
  ctx.log('');
  ctx.log(chalk.bold('Job Details'));
  ctx.log('');
  ctx.log(`  Job ID:     ${job.job_id}`);
  ctx.log(`  Status:     ${getStatusIcon(job.status)} ${job.status}`);
  ctx.log(`  Created:    ${new Date(job.created_at).toLocaleString('ja-JP')}`);

  if (job.started_at) {
    ctx.log(`  Started:    ${new Date(job.started_at).toLocaleString('ja-JP')}`);
  }
  if (job.completed_at) {
    ctx.log(`  Completed:  ${new Date(job.completed_at).toLocaleString('ja-JP')}`);
  }

  if (job.message) {
    ctx.log(`  Message:    ${job.message}`);
  }

  if (job.error) {
    ctx.log('');
    ctx.log(chalk.red(`  Error: ${job.error}`));
  }

  if (job.result) {
    ctx.log('');
    ctx.log(chalk.bold('Results:'));
    ctx.log(`  Total Return:  ${formatPercent(job.result.total_return)}`);
    ctx.log(`  Sharpe Ratio:  ${formatNumber(job.result.sharpe_ratio)}`);
    ctx.log(`  Calmar Ratio:  ${formatNumber(job.result.calmar_ratio)}`);
    ctx.log(`  Max Drawdown:  ${formatPercent(job.result.max_drawdown, true)}`);
    ctx.log(`  Win Rate:      ${formatPercent(job.result.win_rate)}`);
    ctx.log(`  Trade Count:   ${job.result.trade_count}`);

    if (job.result.html_path) {
      ctx.log('');
      ctx.log(chalk.dim(`HTML Report: ${job.result.html_path}`));
    }
  }
}

function getStatusIcon(status: string): string {
  switch (status) {
    case 'completed':
      return chalk.green('✓');
    case 'failed':
      return chalk.red('✗');
    case 'running':
      return chalk.blue('⟳');
    case 'pending':
      return chalk.yellow('○');
    default:
      return chalk.dim('?');
  }
}

function formatPercent(value: number, negative = false): string {
  const color = negative ? (value < 0 ? chalk.green : chalk.red) : value >= 0 ? chalk.green : chalk.red;
  return color(`${(value * 100).toFixed(2)}%`);
}

function formatNumber(value: number): string {
  const color = value >= 0 ? chalk.green : chalk.red;
  return color(value.toFixed(3));
}
