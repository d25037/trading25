/**
 * JQuants Fetch - Test Data Command
 * Fetch Toyota (7203) test data for TA unit tests
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';
import { ApiClient } from '../../../utils/api-client.js';
import { CLI_NAME } from '../../../utils/constants.js';
import { CsvExporter } from '../../../utils/csv-exporter.js';
import { handleApiError } from '../index.js';
import type { TestDataOptions } from './types.js';
export async function fetchToyotaTestData(options: TestDataOptions): Promise<void> {
  const spinner = ora('Fetching Toyota (7203) test data...').start();

  try {
    const apiClient = new ApiClient();

    const to = new Date();
    const from = new Date();
    from.setDate(from.getDate() - Number.parseInt(options.days, 10));

    const params = {
      code: '7203',
      from: from.toISOString().split('T')[0],
      to: to.toISOString().split('T')[0],
    };

    const response = await apiClient.jquants.getDailyQuotes('7203', params);

    if (response.data && response.data.length > 0) {
      spinner.succeed(chalk.green(`Fetched ${response.data.length} days of Toyota data`));

      const quotesForExport = response.data.map((item) => ({
        Code: '7203',
        Date: item.time,
        O: item.open,
        H: item.high,
        L: item.low,
        C: item.close,
        Vo: item.volume ?? null,
      }));

      const exporter = new CsvExporter('packages/shared/src/ta/__fixtures__');
      const filepath = await exporter.exportDailyQuotes(quotesForExport, 'toyota_7203_daily.csv');
      console.log(chalk.green(`✅ Test data saved to: ${filepath}`));

      console.log(chalk.cyan('\n📊 Data Summary'));
      console.log(chalk.white('━'.repeat(50)));
      console.log(chalk.yellow('Stock:'), 'Toyota Motor Corporation (7203)');
      console.log(chalk.yellow('Period:'), `${params.from} to ${params.to}`);
      console.log(chalk.yellow('Records:'), response.data.length);

      const firstQuote = quotesForExport[0];
      const lastQuote = quotesForExport[quotesForExport.length - 1];

      if (firstQuote && lastQuote) {
        console.log(chalk.yellow('First Close:'), `¥${firstQuote.C?.toLocaleString()}`);
        console.log(chalk.yellow('Last Close:'), `¥${lastQuote.C?.toLocaleString()}`);
      }
    } else {
      spinner.warn(chalk.yellow('No data found for Toyota (7203)'));
    }
  } catch (error) {
    spinner.fail();
    handleApiError(error, 'Failed to fetch test data');
  }
}

/**
 * Test data command definition
 */
export const testDataCommand = define({
  name: 'test-data',
  description: 'Fetch Toyota (7203) test data for TA unit tests',
  args: {
    days: {
      type: 'string',
      description: 'Number of days to fetch',
      default: '365',
    },
  },
  examples: `
# Fetch 1 year of Toyota data (default)
${CLI_NAME} jquants fetch test-data

# Fetch 30 days of data
${CLI_NAME} jquants fetch test-data --days 30
  `.trim(),
  run: async (ctx) => {
    const { days } = ctx.values;
    await fetchToyotaTestData({ days: days || '365' });
  },
});
