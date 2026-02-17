/**
 * Market Screening Command
 * Run production strategy-driven screening via API
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';
import { ApiClient } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { CLIError } from '../../utils/error-handling.js';
import { formatResults } from '../screening/output-formatter.js';

interface ScreeningOptions {
  strategies?: string;
  recentDays?: string;
  date?: string;
  markets?: string;
  backtestMetric?: string;
  format?: string;
  sortBy?: string;
  order?: string;
  limit?: string;
  debug?: boolean;
  verbose?: boolean;
}

/**
 * Print debug configuration
 */
function printDebugConfig(options: ScreeningOptions): void {
  console.log(chalk.gray('Using API endpoint for screening'));
  console.log(chalk.gray('Screening configuration:'));
  console.log(chalk.gray(`  Markets: ${options.markets || 'prime'}`));
  console.log(chalk.gray(`  Recent Days: ${options.recentDays || '10'}`));
  console.log(chalk.gray(`  Backtest Metric: ${options.backtestMetric || 'sharpe_ratio'}`));
  console.log(chalk.gray(`  Strategies: ${options.strategies || '(all production)'}`));
  if (options.date) {
    console.log(chalk.gray(`  Reference Date: ${options.date}`));
  }
}

/**
 * Print verbose summary
 */
function printVerboseSummary(response: {
  markets: string[];
  recentDays: number;
  referenceDate?: string;
  backtestMetric: string;
  summary: {
    totalStocksScreened: number;
    skippedCount: number;
    byStrategy: Record<string, number>;
    strategiesEvaluated: string[];
    strategiesWithoutBacktestMetrics: string[];
    warnings: string[];
  };
}): void {
  console.log(chalk.cyan('\n📊 Screening Summary:'));
  console.log(chalk.white('━'.repeat(40)));
  console.log(chalk.yellow('Markets:'), response.markets.join(', '));
  console.log(chalk.yellow('Recent Days:'), response.recentDays);
  console.log(chalk.yellow('Backtest Metric:'), response.backtestMetric);
  if (response.referenceDate) {
    console.log(chalk.yellow('Reference Date:'), response.referenceDate);
  }
  console.log(chalk.yellow('Total Screened:'), response.summary.totalStocksScreened.toLocaleString());
  console.log(chalk.yellow('Skipped:'), response.summary.skippedCount.toLocaleString());
  console.log(chalk.yellow('Strategies Evaluated:'), response.summary.strategiesEvaluated.length);
  console.log(chalk.yellow('Without Metrics:'), response.summary.strategiesWithoutBacktestMetrics.length);

  const topStrategies = Object.entries(response.summary.byStrategy)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5);
  if (topStrategies.length > 0) {
    console.log(chalk.yellow('Top Strategy Hits:'), topStrategies.map(([name, count]) => `${name}(${count})`).join(', '));
  }

  if (response.summary.warnings.length > 0) {
    console.log(chalk.yellow('Warnings:'), response.summary.warnings.length);
  }
}

/**
 * Build API request parameters
 */
export function buildApiParams(options: ScreeningOptions) {
  return {
    markets: options.markets,
    strategies: options.strategies,
    recentDays: options.recentDays ? Number.parseInt(options.recentDays, 10) : undefined,
    date: options.date,
    backtestMetric: options.backtestMetric as
      | 'sharpe_ratio'
      | 'calmar_ratio'
      | 'total_return'
      | 'win_rate'
      | 'profit_factor'
      | undefined,
    sortBy: options.sortBy as 'bestStrategyScore' | 'matchedDate' | 'stockCode' | 'matchStrategyCount' | undefined,
    order: options.order as 'asc' | 'desc' | undefined,
    limit: options.limit ? Number.parseInt(options.limit, 10) : undefined,
  };
}

/**
 * Execute market screening analysis via API
 */
async function executeMarketScreening(options: ScreeningOptions): Promise<void> {
  const apiClient = new ApiClient();

  if (options.debug) {
    printDebugConfig(options);
  }

  const spinnerText = options.date
    ? `Running historical screening (${options.date}) via API...`
    : 'Running screening analysis via API...';
  const spinner = ora(spinnerText).start();

  try {
    const response = await apiClient.analytics.runMarketScreening(buildApiParams(options));

    spinner.succeed(chalk.green(`Screening completed: ${response.summary.matchCount} matches found`));

    if (options.verbose) {
      printVerboseSummary(response);
    }

    if (response.results.length === 0) {
      console.log(chalk.yellow('\n🔍 No stocks matched the screening criteria'));
      return;
    }

    console.log(chalk.cyan(`\n📈 Market Screening Results: ${response.results.length} stocks returned`));
    console.log(chalk.white('━'.repeat(80)));

    await formatResults(response.results, {
      format: (options.format || 'table') as 'table' | 'json' | 'csv',
      verbose: options.verbose || false,
      debug: options.debug || false,
    });
  } catch (error) {
    spinner.fail(chalk.red('Screening failed'));
    throw error;
  }
}

/**
 * Handle screening errors
 */
function handleScreeningError(error: unknown): void {
  console.error(chalk.red('\n❌ Market screening failed:'));

  if (error instanceof Error) {
    console.error(chalk.red(`   Error: ${error.message}`));

    if (process.env.DEBUG) {
      console.error(chalk.gray('   Stack trace:'));
      console.error(chalk.gray(error.stack));
    }
  } else {
    const errorString = String(error);
    console.error(chalk.red(`   ${errorString}`));
  }

  console.error(chalk.gray('\n💡 Troubleshooting tips:'));
  console.error(chalk.gray('   • Ensure the API server is running: uv run bt server --port 3002'));
  console.error(chalk.gray(`   • Ensure market.db exists: ${CLI_NAME} db sync`));
  console.error(chalk.gray('   • Try with --debug flag for more information'));
}

/**
 * Screening command definition
 */
export const screeningCommand = define({
  name: 'screening',
  description: 'Run strategy-driven stock screening analysis on market-wide data',
  args: {
    markets: {
      type: 'string',
      description: 'Market filter (prime|standard|growth|0111|0112|0113, comma-separated)',
      default: 'prime',
    },
    strategies: {
      type: 'string',
      description: 'Production strategies (comma-separated, optional; empty means all production)',
    },
    recentDays: {
      type: 'string',
      description: 'Days to look back for recent signals',
      default: '10',
    },
    date: {
      type: 'string',
      description: 'Reference date for historical screening (YYYY-MM-DD)',
    },
    backtestMetric: {
      type: 'string',
      description: 'Backtest metric (sharpe_ratio|calmar_ratio|total_return|win_rate|profit_factor)',
      default: 'sharpe_ratio',
    },
    format: {
      type: 'string',
      description: 'Output format (table|json|csv)',
      default: 'table',
    },
    sortBy: {
      type: 'string',
      description: 'Sort results by field (bestStrategyScore|matchedDate|stockCode|matchStrategyCount)',
      default: 'bestStrategyScore',
    },
    order: {
      type: 'string',
      description: 'Sort order (asc|desc)',
      default: 'desc',
    },
    limit: {
      type: 'string',
      description: 'Limit number of results',
    },
    debug: {
      type: 'boolean',
      description: 'Enable debug output',
    },
    verbose: {
      type: 'boolean',
      description: 'Enable verbose output',
    },
  },
  examples: `
# Basic screening (Prime market, all production strategies)
${CLI_NAME} analysis screening

# Restrict to selected production strategies
${CLI_NAME} analysis screening --strategies range_break_v15,forward_eps_driven

# Use different backtest metric and sorting
${CLI_NAME} analysis screening --backtest-metric calmar_ratio --sort-by matchedDate --order asc

# Historical screening for multiple markets
${CLI_NAME} analysis screening --date 2025-12-30 --markets prime,standard

# JSON output with limit
${CLI_NAME} analysis screening --format json --limit 100
  `.trim(),
  run: async (ctx) => {
    const {
      markets,
      strategies,
      recentDays,
      date,
      backtestMetric,
      format,
      sortBy,
      order,
      limit,
      debug,
      verbose,
    } = ctx.values;

    try {
      await executeMarketScreening({
        strategies,
        recentDays,
        date,
        markets,
        backtestMetric,
        format,
        sortBy,
        order,
        limit,
        debug,
        verbose,
      });
    } catch (error: unknown) {
      handleScreeningError(error);
      throw new CLIError('Screening failed', 1, true, { cause: error });
    }
  },
});
