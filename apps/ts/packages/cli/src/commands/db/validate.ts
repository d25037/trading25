/**
 * Market Validate Command
 * Validate market database integrity and completeness
 * Uses API endpoint: GET /api/market/validate
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';
import {
  type AdjustmentEvent,
  ApiClient,
  type IntegrityIssue,
  type MarketValidationResponse,
} from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { CLIError } from '../../utils/error-handling.js';

/**
 * Display initialization status
 */
function displayInitializationStatus(data: MarketValidationResponse): void {
  console.log(chalk.white(`Initialized: ${data.initialized ? chalk.green('✓ Yes') : chalk.red('✗ No')}`));

  if (data.lastSync) {
    const syncDate = new Date(data.lastSync);
    console.log(chalk.white(`Last Sync: ${chalk.yellow(syncDate.toLocaleString())}`));
  }

  if (data.lastStocksRefresh) {
    const refreshDate = new Date(data.lastStocksRefresh);
    console.log(chalk.white(`Last Stocks Refresh: ${chalk.yellow(refreshDate.toLocaleString())}`));
  }
}

/**
 * Display TOPIX data validation
 */
function displayTopixValidation(data: MarketValidationResponse): void {
  console.log(chalk.bold('\n📊 TOPIX Data:'));
  const { topix } = data;

  if (topix.count > 0 && topix.dateRange) {
    console.log(chalk.green(`  ✓ ${topix.count} days of data (${topix.dateRange.min} to ${topix.dateRange.max})`));
  } else {
    console.log(chalk.red('  ✗ No TOPIX data found'));
  }
}

/**
 * Display stock list validation
 */
function displayStockListValidation(data: MarketValidationResponse): void {
  console.log(chalk.bold('\n📈 Stock List:'));
  const { stocks } = data;

  if (stocks.total > 0) {
    console.log(chalk.green(`  ✓ ${stocks.total} stocks`));

    const markets = Object.entries(stocks.byMarket)
      .map(([market, count]) => `${market}: ${count}`)
      .join(', ');
    console.log(chalk.white(`    Markets: ${markets}`));
  } else {
    console.log(chalk.red('  ✗ No stocks found'));
  }
}

/**
 * Display stock price data validation
 */
function displayStockPriceValidation(data: MarketValidationResponse): void {
  console.log(chalk.bold('\n💹 Stock Price Data:'));
  const { stockData } = data;

  if (stockData.count > 0 && stockData.dateRange) {
    console.log(
      chalk.green(`  ✓ ${stockData.count} days of data (${stockData.dateRange.min} to ${stockData.dateRange.max})`)
    );

    if (stockData.missingDatesCount === 0) {
      console.log(chalk.green('  ✓ No gaps detected'));
    } else {
      console.log(chalk.yellow(`  ⚠ Missing ${stockData.missingDatesCount} dates:`));
      const dateStrings = stockData.missingDates.slice(0, 10).join(', ');
      console.log(chalk.gray(`    ${dateStrings}`));

      if (stockData.missingDatesCount > 10) {
        console.log(chalk.gray(`    ... and ${stockData.missingDatesCount - 10} more`));
      }
    }
  } else {
    console.log(chalk.red('  ✗ No stock price data found'));
  }
}

/**
 * Display failed dates
 */
function displayFailedDates(data: MarketValidationResponse): void {
  if (data.failedDatesCount > 0) {
    console.log(chalk.bold('\n⚠️  Failed Dates in Retry Queue:'));
    console.log(chalk.yellow(`  ${data.failedDatesCount} dates pending retry`));

    const dateStrings = data.failedDates.slice(0, 5).join(', ');
    console.log(chalk.gray(`    ${dateStrings}`));

    if (data.failedDatesCount > 5) {
      console.log(chalk.gray(`    ... and ${data.failedDatesCount - 5} more`));
    }
  }
}

/**
 * Display stock split/merger events
 */
function displayAdjustmentEvents(events: AdjustmentEvent[], totalCount: number): void {
  console.log(chalk.bold('\n📊 Stock Split/Merger Events:'));

  if (totalCount === 0) {
    console.log(chalk.green('  ✓ No adjustment events detected'));
    return;
  }

  console.log(chalk.yellow(`  ⚠ Detected ${totalCount} adjustment events (most recent 20 shown)`));
  console.log();
  console.log(chalk.white('  Code    Date         Factor   Close      Event Type'));
  console.log(chalk.gray('  ─'.repeat(60)));

  for (const event of events.slice(0, 10)) {
    const factor = event.adjustmentFactor.toFixed(3).padEnd(8);
    const close = event.close.toFixed(2).padStart(10);

    console.log(chalk.white(`  ${event.code.padEnd(6)}  ${event.date}   ${factor} ${close}   ${event.eventType}`));
  }

  if (events.length > 10) {
    console.log(chalk.gray(`  ... and ${events.length - 10} more`));
  }
}

/**
 * Display data integrity check
 */
function displayDataIntegrity(issues: IntegrityIssue[], totalCount: number): void {
  console.log(chalk.bold('\n🔍 Data Integrity:'));

  if (totalCount > 0) {
    console.log(chalk.red(`  ✗ Found ${totalCount} stocks with data outside TOPIX range (data integrity issue)`));
    for (const issue of issues.slice(0, 5)) {
      console.log(chalk.gray(`    ${issue.code}: ${issue.count} records`));
    }
    if (totalCount > 5) {
      console.log(chalk.gray(`    ... and ${totalCount - 5} more`));
    }
  } else {
    console.log(chalk.green('  ✓ All stock data within TOPIX date range'));
  }
}

/**
 * Display recommendations
 */
function displayRecommendations(recommendations: string[]): void {
  console.log(chalk.bold('\n💡 Recommendations:'));

  for (const rec of recommendations) {
    if (rec.includes('complete and up to date')) {
      console.log(chalk.green(`  ✓ ${rec}`));
    } else if (rec.includes('integrity issue')) {
      console.log(chalk.red(`  • ${rec}`));
    } else {
      console.log(chalk.yellow(`  • ${rec}`));
    }
  }
}

/**
 * Display status with color
 */
function displayStatus(status: 'healthy' | 'warning' | 'error'): void {
  const statusText =
    status === 'healthy'
      ? chalk.green('✓ Healthy')
      : status === 'warning'
        ? chalk.yellow('⚠ Warning')
        : chalk.red('✗ Error');
  console.log(chalk.white(`Status: ${statusText}`));
}

/**
 * Validate command definition
 */
export const validateCommand = define({
  name: 'validate',
  description: 'Validate market database integrity',
  examples: `
# Validate market database
${CLI_NAME} db validate
  `.trim(),
  run: async () => {
    const apiClient = new ApiClient();
    const spinner = ora('Validating market database...').start();

    try {
      const data = await apiClient.validateMarketDatabase();
      spinner.succeed(chalk.green('Validation complete'));

      console.log(chalk.bold(`\n${'='.repeat(60)}`));
      console.log(chalk.bold.cyan('Market Database Validation Report'));
      console.log(chalk.bold('='.repeat(60)));

      displayStatus(data.status);
      displayInitializationStatus(data);
      displayTopixValidation(data);
      displayStockListValidation(data);
      displayStockPriceValidation(data);
      displayFailedDates(data);
      displayAdjustmentEvents(data.adjustmentEvents, data.adjustmentEventsCount);
      displayDataIntegrity(data.integrityIssues, data.integrityIssuesCount);
      displayRecommendations(data.recommendations);

      console.log(`${chalk.bold('='.repeat(60))}\n`);
    } catch (error) {
      spinner.fail(chalk.red('Validation failed'));

      console.error(chalk.red('\n❌ Market validation failed:'));

      if (error instanceof Error) {
        console.error(chalk.red(`   Error: ${error.message}`));
      } else {
        console.error(chalk.red(`   ${String(error)}`));
      }

      console.error(chalk.gray('\n💡 Troubleshooting tips:'));
      console.error(chalk.gray('   • Ensure API server is running: bun dev:api'));
      console.error(chalk.gray(`   • Ensure market.db exists: ${CLI_NAME} db sync --init`));
      throw new CLIError('Validation failed', 1, true, { cause: error });
    }
  },
});
