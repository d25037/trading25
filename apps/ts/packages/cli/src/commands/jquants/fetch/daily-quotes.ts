/**
 * JQuants Fetch - Daily Quotes Command
 * Fetch daily stock quotes
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';
import { ApiClient } from '../../../utils/api-client.js';
import { CLI_NAME } from '../../../utils/constants.js';
import { CsvExporter } from '../../../utils/csv-exporter.js';
import { CLIValidationError } from '../../../utils/error-handling.js';
import { displayDataSummary } from '../helpers.js';
import { handleApiError } from '../index.js';
import { displayDailyQuotes } from './display.js';
import type { FetchOptions } from './types.js';
export async function fetchDailyQuotes(code: string, options: FetchOptions): Promise<void> {
  const spinner = ora(`Fetching daily quotes for ${code}...`).start();

  try {
    const apiClient = new ApiClient();

    const params: { from?: string; to?: string; date?: string } = {};
    if (options.from) params.from = options.from;
    if (options.to) params.to = options.to;
    if (options.date) params.date = options.date;

    const response = await apiClient.getDailyQuotes(code, params);

    if (response.data && response.data.length > 0) {
      spinner.succeed(chalk.green(`Fetched ${response.data.length} daily quotes`));

      // Convert API response format to JQuants v2 format for display
      const quotesForDisplay = response.data.map((item) => ({
        Code: code,
        Date: item.time,
        O: item.open,
        H: item.high,
        L: item.low,
        C: item.close,
        Vo: item.volume,
      }));

      // Export to CSV/JSON
      const exportType = options.csv ? 'csv' : 'json';
      const exporter = new CsvExporter(options.output);

      if (exportType === 'csv') {
        const timestamp = new Date().toISOString().split('T')[0];
        const filename = `${code}_daily_${timestamp}.csv`;
        const filepath = await exporter.exportDailyQuotes(quotesForDisplay, filename);
        console.log(chalk.green(`✅ CSV exported to: ${filepath}`));
      } else {
        const timestamp = new Date().toISOString().split('T')[0];
        const filename = `${code}_daily_${timestamp}.json`;
        const filepath = await exporter.exportJSON(quotesForDisplay, filename);
        console.log(chalk.green(`✅ JSON exported to: ${filepath}`));
      }

      displayDataSummary(quotesForDisplay, displayDailyQuotes);
    } else {
      spinner.warn(chalk.yellow('No data found'));
    }
  } catch (error) {
    spinner.fail();
    handleApiError(error, 'Failed to fetch daily quotes');
  }
}

/**
 * Daily quotes command definition
 */
export const dailyQuotesCommand = define({
  name: 'daily-quotes',
  description: 'Fetch daily stock quotes',
  args: {
    code: {
      type: 'positional',
      description: 'Stock code',
    },
    date: {
      type: 'string',
      short: 'd',
      description: 'Specific date (YYYY-MM-DD)',
    },
    from: {
      type: 'string',
      short: 'f',
      description: 'From date (YYYY-MM-DD)',
    },
    to: {
      type: 'string',
      short: 't',
      description: 'To date (YYYY-MM-DD)',
    },
    csv: {
      type: 'boolean',
      description: 'Export as CSV',
    },
    json: {
      type: 'boolean',
      description: 'Export as JSON (default)',
    },
    output: {
      type: 'string',
      short: 'o',
      description: 'Output directory',
      default: './data',
    },
  },
  examples: `
# Fetch daily quotes for Toyota
${CLI_NAME} jquants fetch daily-quotes 7203

# Fetch with date range
${CLI_NAME} jquants fetch daily-quotes 7203 -f 2024-01-01 -t 2024-12-31

# Export as CSV
${CLI_NAME} jquants fetch daily-quotes 7203 --csv
  `.trim(),
  run: async (ctx) => {
    const { code, date, from, to, csv, json, output } = ctx.values;

    if (!code) {
      throw new CLIValidationError('Stock code is required');
    }

    await fetchDailyQuotes(code, { date, from, to, csv, json, output: output || './data' });
  },
});
