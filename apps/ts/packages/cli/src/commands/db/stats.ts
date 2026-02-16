/**
 * Market Database Stats Command
 * Display statistics about the market database
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';
import { ApiClient, type MarketStatsResponse } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { CLIError } from '../../utils/error-handling.js';

/**
 * Format bytes to human readable size
 */
function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / k ** i).toFixed(2)} ${sizes[i]}`;
}

/**
 * Format number with commas
 */
function formatNumber(num: number): string {
  return num.toLocaleString();
}

/**
 * Display stats in table format
 */
function displayTableFormat(stats: MarketStatsResponse): void {
  console.log(`\n${chalk.bold('='.repeat(60))}`);
  console.log(chalk.bold.cyan('Market Database Statistics'));
  console.log(chalk.bold('='.repeat(60)));

  // Basic Info
  console.log(chalk.yellow('\n--- Basic Info ---'));
  console.log(`Initialized:    ${stats.initialized ? chalk.green('Yes') : chalk.red('No')}`);
  console.log(`Database Size:  ${chalk.white(formatBytes(stats.databaseSize))}`);
  console.log(`Last Sync:      ${stats.lastSync ? chalk.white(stats.lastSync) : chalk.gray('Never')}`);

  // TOPIX
  console.log(chalk.yellow('\n--- TOPIX Data ---'));
  console.log(`Records:        ${chalk.white(formatNumber(stats.topix.count))}`);
  if (stats.topix.dateRange) {
    console.log(`Date Range:     ${chalk.white(`${stats.topix.dateRange.min} ~ ${stats.topix.dateRange.max}`)}`);
  }

  // Stocks
  console.log(chalk.yellow('\n--- Stock List ---'));
  console.log(`Total Stocks:   ${chalk.white(formatNumber(stats.stocks.total))}`);
  for (const [market, count] of Object.entries(stats.stocks.byMarket)) {
    console.log(`  ${market}:${' '.repeat(12 - market.length)}${chalk.white(formatNumber(count))}`);
  }

  // Stock Data
  console.log(chalk.yellow('\n--- Stock Data ---'));
  console.log(`Total Records:  ${chalk.white(formatNumber(stats.stockData.count))}`);
  console.log(`Trading Days:   ${chalk.white(formatNumber(stats.stockData.dateCount))}`);
  console.log(`Avg/Day:        ${chalk.white(formatNumber(stats.stockData.averageStocksPerDay))}`);
  if (stats.stockData.dateRange) {
    console.log(
      `Date Range:     ${chalk.white(`${stats.stockData.dateRange.min} ~ ${stats.stockData.dateRange.max}`)}`
    );
  }

  // Indices
  console.log(chalk.yellow('\n--- Indices Data ---'));
  console.log(`Index Master:   ${chalk.white(formatNumber(stats.indices.masterCount))}`);
  console.log(`Data Records:   ${chalk.white(formatNumber(stats.indices.dataCount))}`);
  console.log(`Trading Days:   ${chalk.white(formatNumber(stats.indices.dateCount))}`);
  if (stats.indices.dateRange) {
    console.log(`Date Range:     ${chalk.white(`${stats.indices.dateRange.min} ~ ${stats.indices.dateRange.max}`)}`);
  }
  if (Object.keys(stats.indices.byCategory).length > 0) {
    console.log(`By Category:`);
    for (const [category, count] of Object.entries(stats.indices.byCategory)) {
      console.log(`  ${category}:${' '.repeat(12 - category.length)}${chalk.white(formatNumber(count))}`);
    }
  }

  console.log(`\n${chalk.bold('='.repeat(60))}\n`);
}

/**
 * Display stats in JSON format
 */
function displayJsonFormat(stats: MarketStatsResponse): void {
  console.log(JSON.stringify(stats, null, 2));
}

/**
 * Stats command definition
 */
export const statsCommand = define({
  name: 'stats',
  description: 'Display market database statistics',
  args: {
    json: {
      type: 'boolean',
      description: 'Output in JSON format',
    },
  },
  examples: `
# Display stats in table format
${CLI_NAME} db stats

# Display stats in JSON format
${CLI_NAME} db stats --json
  `.trim(),
  run: async (ctx) => {
    const { json } = ctx.values;
    const spinner = ora('Fetching market database statistics...').start();

    try {
      const apiClient = new ApiClient();
      const stats = await apiClient.database.getMarketStats();

      spinner.stop();

      if (json) {
        displayJsonFormat(stats);
      } else {
        displayTableFormat(stats);
      }
    } catch (error) {
      spinner.fail('Failed to get statistics');

      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error(chalk.red(`\nError: ${errorMessage}`));

      console.error(chalk.gray('\nTroubleshooting tips:'));
      console.error(chalk.gray('   - Ensure the API server is running: uv run bt server --port 3002'));
      console.error(chalk.gray(`   - Run "${CLI_NAME} db sync --init" if database is not initialized`));

      throw new CLIError(errorMessage, 1, true, { cause: error });
    }
  },
});
