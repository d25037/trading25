/**
 * JQuants Fetch - Margin Interest Command
 * Fetch weekly margin interest data
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
import { displayMarginInterest } from './display.js';
import type { FetchOptions } from './types.js';
export async function fetchMarginInterest(code: string, options: FetchOptions): Promise<void> {
  const spinner = ora(`Fetching margin interest for ${code}...`).start();

  try {
    const apiClient = new ApiClient();

    const params: { from?: string; to?: string; date?: string } = {};
    if (options.from) params.from = options.from;
    if (options.to) params.to = options.to;
    if (options.date) params.date = options.date;

    const response = await apiClient.getMarginInterest(code, params);

    if (response.marginInterest && response.marginInterest.length > 0) {
      spinner.succeed(chalk.green(`Fetched ${response.marginInterest.length} margin records`));

      // Convert API response format to JQuants v2 format for display
      const marginForDisplay = response.marginInterest.map((item) => ({
        Code: code,
        Date: item.date,
        ShrtVol: item.shortMarginTradeVolume,
        LongVol: item.longMarginTradeVolume,
      }));

      // Export to CSV/JSON
      const exportType = options.csv ? 'csv' : 'json';
      const exporter = new CsvExporter(options.output);

      if (exportType === 'csv') {
        const timestamp = new Date().toISOString().split('T')[0];
        const filename = `${code}_margin_${timestamp}.csv`;
        const filepath = await exporter.exportWeeklyMarginInterest(marginForDisplay, filename);
        console.log(chalk.green(`✅ CSV exported to: ${filepath}`));
      } else {
        const timestamp = new Date().toISOString().split('T')[0];
        const filename = `${code}_margin_${timestamp}.json`;
        const filepath = await exporter.exportJSON(marginForDisplay, filename);
        console.log(chalk.green(`✅ JSON exported to: ${filepath}`));
      }

      displayDataSummary(marginForDisplay, displayMarginInterest);
    } else {
      spinner.warn(chalk.yellow('No data found'));
    }
  } catch (error) {
    spinner.fail();
    handleApiError(error, 'Failed to fetch margin interest');
  }
}

/**
 * Margin interest command definition
 */
export const marginCommand = define({
  name: 'margin',
  description: 'Fetch weekly margin interest data',
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
# Fetch margin interest data for Toyota
${CLI_NAME} jquants fetch margin 7203

# Fetch with date range
${CLI_NAME} jquants fetch margin 7203 -f 2024-01-01 -t 2024-12-31

# Export as CSV
${CLI_NAME} jquants fetch margin 7203 --csv
  `.trim(),
  run: async (ctx) => {
    const { code, date, from, to, csv, json, output } = ctx.values;

    if (!code) {
      throw new CLIValidationError('Stock code is required');
    }

    await fetchMarginInterest(code, { date, from, to, csv, json, output: output || './data' });
  },
});
