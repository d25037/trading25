/**
 * Market Screening Command
 * Run stock screening on market.db data via API
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';
import { ApiClient } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { CLIError } from '../../utils/error-handling.js';
import { formatResults } from '../screening/output-formatter.js';

interface ScreeningOptions {
  rangeBreakFast?: boolean;
  rangeBreakSlow?: boolean;
  recentDays?: string;
  date?: string;
  markets?: string;
  format?: string;
  sortBy?: string;
  order?: string;
  minBreakPercentage?: string;
  minVolumeRatio?: string;
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
  console.log(chalk.gray(`  Range Break Fast: ${options.rangeBreakFast !== false}`));
  console.log(chalk.gray(`  Range Break Slow: ${options.rangeBreakSlow !== false}`));
  console.log(chalk.gray(`  Recent Days: ${options.recentDays || '10'}`));
  console.log(chalk.gray(`  Markets: ${options.markets || 'prime'}`));
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
  summary: {
    totalStocksScreened: number;
    skippedCount: number;
    byScreeningType: { rangeBreakFast: number; rangeBreakSlow: number };
  };
}): void {
  console.log(chalk.cyan('\n📊 Screening Summary:'));
  console.log(chalk.white('━'.repeat(40)));
  console.log(chalk.yellow('Markets:'), response.markets.join(', '));
  console.log(chalk.yellow('Recent Days:'), response.recentDays);
  if (response.referenceDate) {
    console.log(chalk.yellow('Reference Date:'), response.referenceDate);
  }
  console.log(chalk.yellow('Total Screened:'), response.summary.totalStocksScreened.toLocaleString());
  console.log(chalk.yellow('Skipped:'), response.summary.skippedCount.toLocaleString());
  console.log(chalk.yellow('Range Break Fast:'), response.summary.byScreeningType.rangeBreakFast);
  console.log(chalk.yellow('Range Break Slow:'), response.summary.byScreeningType.rangeBreakSlow);
}

/**
 * Build API request parameters
 */
function buildApiParams(options: ScreeningOptions) {
  return {
    markets: options.markets,
    rangeBreakFast: options.rangeBreakFast,
    rangeBreakSlow: options.rangeBreakSlow,
    recentDays: options.recentDays ? Number.parseInt(options.recentDays, 10) : undefined,
    date: options.date,
    minBreakPercentage: options.minBreakPercentage ? Number.parseFloat(options.minBreakPercentage) : undefined,
    minVolumeRatio: options.minVolumeRatio ? Number.parseFloat(options.minVolumeRatio) : undefined,
    sortBy: options.sortBy as 'date' | 'stockCode' | 'volumeRatio' | 'breakPercentage' | undefined,
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
    const response = await apiClient.runMarketScreening(buildApiParams(options));

    spinner.succeed(chalk.green(`Screening completed: ${response.summary.matchCount} matches found`));

    if (options.verbose) {
      printVerboseSummary(response);
    }

    if (response.results.length === 0) {
      console.log(chalk.yellow('\n🔍 No stocks matched the screening criteria'));
      return;
    }

    console.log(chalk.cyan(`\n📈 Market Screening Results: ${response.results.length} stocks matched`));
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
  console.error(chalk.gray('   • Ensure the API server is running: bun run dev:api'));
  console.error(chalk.gray(`   • Ensure market.db exists: ${CLI_NAME} db sync`));
  console.error(chalk.gray('   • Try with --debug flag for more information'));
}

/**
 * Screening command definition
 */
export const screeningCommand = define({
  name: 'screening',
  description: 'Run stock screening analysis on market-wide data',
  args: {
    markets: {
      type: 'string',
      description: 'Market filter (prime|standard|prime,standard)',
      default: 'prime',
    },
    rangeBreakFast: {
      type: 'boolean',
      description: 'Enable range break Fast screening (EMA30/120, 1.7)',
      default: true,
    },
    rangeBreakSlow: {
      type: 'boolean',
      description: 'Enable range break Slow screening (SMA50/150, 1.7)',
      default: true,
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
    format: {
      type: 'string',
      description: 'Output format (table|json|csv)',
      default: 'table',
    },
    sortBy: {
      type: 'string',
      description: 'Sort results by field (date|stockCode|volumeRatio|breakPercentage)',
      default: 'date',
    },
    order: {
      type: 'string',
      description: 'Sort order (asc|desc)',
      default: 'desc',
    },
    minBreakPercentage: {
      type: 'string',
      description: 'Minimum break percentage for range break filtering',
    },
    minVolumeRatio: {
      type: 'string',
      description: 'Minimum volume ratio for filtering',
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
# Basic screening (Prime market only, default)
${CLI_NAME} analysis screening

# Screen Standard market only
${CLI_NAME} analysis screening --markets standard

# Screen both Prime and Standard markets
${CLI_NAME} analysis screening --markets prime,standard

# Run only Range Break Fast screening
${CLI_NAME} analysis screening --no-range-break-slow

# Run only Range Break Slow screening
${CLI_NAME} analysis screening --no-range-break-fast

# Custom filtering and output
${CLI_NAME} analysis screening --min-break-percentage 5 --format json

# Sort by volume ratio
${CLI_NAME} analysis screening --sort-by volumeRatio --order desc

# Historical screening with future returns
${CLI_NAME} analysis screening --date 2024-06-01

# Historical screening for specific market
${CLI_NAME} analysis screening --date 2024-06-01 --markets prime,standard
  `.trim(),
  run: async (ctx) => {
    const {
      markets,
      rangeBreakFast,
      rangeBreakSlow,
      recentDays,
      date,
      format,
      sortBy,
      order,
      minBreakPercentage,
      minVolumeRatio,
      limit,
      debug,
      verbose,
    } = ctx.values;

    try {
      await executeMarketScreening({
        rangeBreakFast,
        rangeBreakSlow,
        recentDays,
        date,
        markets,
        format,
        sortBy,
        order,
        minBreakPercentage,
        minVolumeRatio,
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
