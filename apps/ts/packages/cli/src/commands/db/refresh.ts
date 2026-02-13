/**
 * Market Refresh Command
 * Manually refetch historical data for specific stocks to update adjusted prices via API
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';
import { ApiClient, type MarketRefreshResponse } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { CLIError, CLIValidationError, DB_TIPS, handleCommandError } from '../../utils/error-handling.js';

/**
 * Display refetch result summary
 */
function displayRefetchResult(result: MarketRefreshResponse): void {
  console.log(`\n${chalk.bold('='.repeat(60))}`);
  console.log(chalk.bold.cyan('Market Refresh Summary'));
  console.log(chalk.bold('='.repeat(60)));

  console.log(chalk.white(`Total Stocks: ${chalk.yellow(result.totalStocks.toString())}`));
  console.log(chalk.white(`Successful: ${chalk.green(result.successCount.toString())}`));

  if (result.failedCount > 0) {
    console.log(chalk.white(`Failed: ${chalk.red(result.failedCount.toString())}`));
  }

  console.log(chalk.white(`API Calls: ${chalk.yellow(result.totalApiCalls.toString())}`));
  console.log(chalk.white(`Records Updated: ${chalk.yellow(result.totalRecordsStored.toString())}`));

  if (result.errors.length > 0) {
    console.log(chalk.red('\nErrors:'));
    for (const error of result.errors) {
      console.log(chalk.red(`  • ${error}`));
    }
  }

  console.log(`${chalk.bold('='.repeat(60))}\n`);
}

/**
 * Validate refresh inputs
 */
function validateRefreshInputs(codes: string[]): void {
  if (codes.length === 0) {
    throw new CLIValidationError(
      `No stock codes provided\nUsage: ${CLI_NAME} db refresh <code1> [code2] [code3] ...\nExample: ${CLI_NAME} db refresh 7203 6758 9984`
    );
  }

  if (codes.length > 50) {
    throw new CLIValidationError('Too many stock codes (maximum 50). Please provide at most 50 stock codes at a time.');
  }
}

/**
 * Handle refresh result and exit accordingly
 */
function handleRefreshResult(result: MarketRefreshResponse): void {
  if (result.successCount === result.totalStocks) {
    console.log(chalk.green('✓ All stocks refreshed successfully'));
    return;
  }

  if (result.successCount > 0) {
    console.log(chalk.yellow(`⚠ Partial success: ${result.successCount}/${result.totalStocks} stocks refreshed`));
    throw new CLIError('Partial refresh failure', 1, true);
  }

  console.log(chalk.red('✗ All refresh operations failed'));
  throw new CLIError('All refresh operations failed', 1, true);
}

/**
 * Refresh command definition
 */
export const refreshCommand = define({
  name: 'refresh',
  description: 'Manually refetch historical data for specific stocks',
  args: {
    debug: {
      type: 'boolean',
      description: 'Enable debug logging',
    },
  },
  examples: `
# Refresh single stock
${CLI_NAME} db refresh 7203

# Refresh multiple stocks
${CLI_NAME} db refresh 7203 6758 9984

# With debug logging
${CLI_NAME} db refresh 7203 --debug
  `.trim(),
  run: async (ctx) => {
    const { debug } = ctx.values;
    const codes = ctx.positionals;
    const spinner = ora('Initializing market refresh...').start();

    try {
      validateRefreshInputs(codes);

      if (debug) {
        console.log(chalk.gray('[DEBUG] Using API endpoint for refresh'));
        console.log(chalk.gray(`[DEBUG] Codes: ${codes.join(', ')}`));
      }

      const apiClient = new ApiClient();

      spinner.text = `Refreshing historical data for ${codes.length} stock(s) via API...`;

      const result = await apiClient.database.refreshStocks(codes);

      spinner.stop();
      displayRefetchResult(result);
      handleRefreshResult(result);
    } catch (error) {
      handleCommandError(error, spinner, {
        failMessage: 'Refresh failed',
        debug,
        tips: DB_TIPS.refresh,
      });
    }
  },
});
