/**
 * JQuants Fetch - TOPIX Command
 * Fetch TOPIX index data
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';
import { ApiClient } from '../../../utils/api-client.js';
import { CLI_NAME } from '../../../utils/constants.js';
import { CsvExporter } from '../../../utils/csv-exporter.js';
import { displayDataSummary } from '../helpers.js';
import { handleApiError } from '../index.js';
import { displayTOPIX } from './display.js';
import type { FetchOptions } from './types.js';
export async function fetchTOPIX(options: FetchOptions): Promise<void> {
  const spinner = ora('Fetching TOPIX index data...').start();

  try {
    const apiClient = new ApiClient();

    const params: { from?: string; to?: string; date?: string } = {};
    if (options.from) params.from = options.from;
    if (options.to) params.to = options.to;
    if (options.date) params.date = options.date;

    const response = await apiClient.getTOPIX(params);

    if (response.topix && response.topix.length > 0) {
      spinner.succeed(chalk.green(`Fetched ${response.topix.length} TOPIX records`));

      // Convert API response format to JQuants v2 format for display
      const topixForDisplay = response.topix.map((item) => ({
        Date: item.date,
        O: item.open,
        H: item.high,
        L: item.low,
        C: item.close,
      }));

      // Export to CSV/JSON
      const exportType = options.csv ? 'csv' : 'json';
      const exporter = new CsvExporter(options.output);

      if (exportType === 'csv') {
        const timestamp = new Date().toISOString().split('T')[0];
        const filename = `topix_${timestamp}.csv`;
        const filepath = await exporter.exportTOPIX(topixForDisplay, filename);
        console.log(chalk.green(`✅ CSV exported to: ${filepath}`));
      } else {
        const timestamp = new Date().toISOString().split('T')[0];
        const filename = `topix_${timestamp}.json`;
        const filepath = await exporter.exportJSON(topixForDisplay, filename);
        console.log(chalk.green(`✅ JSON exported to: ${filepath}`));
      }

      displayDataSummary(topixForDisplay, displayTOPIX);
    } else {
      spinner.warn(chalk.yellow('No TOPIX data found'));
    }
  } catch (error) {
    spinner.fail();
    handleApiError(error, 'Failed to fetch TOPIX data');
  }
}

/**
 * TOPIX command definition
 */
export const topixCommand = define({
  name: 'topix',
  description: 'Fetch TOPIX index data',
  args: {
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
# Fetch recent TOPIX data
${CLI_NAME} jquants fetch topix

# Fetch with date range
${CLI_NAME} jquants fetch topix -f 2024-01-01 -t 2024-12-31

# Export as CSV
${CLI_NAME} jquants fetch topix --csv
  `.trim(),
  run: async (ctx) => {
    const { from, to, csv, json, output } = ctx.values;
    await fetchTOPIX({ from, to, csv, json, output: output || './data' });
  },
});
