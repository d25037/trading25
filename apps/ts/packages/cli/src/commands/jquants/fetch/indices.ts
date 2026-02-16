/**
 * JQuants Fetch - Indices Command
 * Fetch index data
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';
import { ApiClient } from '../../../utils/api-client.js';
import { CLI_NAME } from '../../../utils/constants.js';
import { CsvExporter } from '../../../utils/csv-exporter.js';
import { displayDataSummary } from '../helpers.js';
import { handleApiError } from '../index.js';
import { displayIndices } from './display.js';
import type { FetchOptions } from './types.js';
export async function fetchIndices(options: FetchOptions): Promise<void> {
  const spinner = ora('Fetching index data...').start();

  try {
    const apiClient = new ApiClient();

    const params: { code?: string; from?: string; to?: string; date?: string } = {};
    if (options.code) params.code = options.code;
    if (options.from) params.from = options.from;
    if (options.to) params.to = options.to;
    if (options.date) params.date = options.date;

    const response = await apiClient.jquants.getIndices(params);

    if (response.indices && response.indices.length > 0) {
      spinner.succeed(chalk.green(`Fetched ${response.indices.length} index records`));

      // Convert API response format to JQuants v2 format for display
      const indicesForDisplay = response.indices.map((item) => ({
        Date: item.date,
        Code: item.code || '',
        O: item.open,
        H: item.high,
        L: item.low,
        C: item.close,
      }));

      // Export to JSON if requested
      if (options.json) {
        const exporter = new CsvExporter(options.output);
        const timestamp = new Date().toISOString().split('T')[0];
        const filename = `indices_${timestamp}.json`;
        const filepath = await exporter.exportJSON(indicesForDisplay, filename);
        console.log(chalk.green(`✅ JSON exported to: ${filepath}`));
      }

      displayDataSummary(indicesForDisplay, displayIndices);
    } else {
      spinner.warn(chalk.yellow('No data found'));
    }
  } catch (error) {
    spinner.fail();
    handleApiError(error, 'Failed to fetch indices');
  }
}

/**
 * Indices command definition
 */
export const indicesCommand = define({
  name: 'indices',
  description: 'Fetch index data',
  args: {
    code: {
      type: 'string',
      short: 'c',
      description: 'Index code',
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
    json: {
      type: 'boolean',
      description: 'Export as JSON',
    },
    output: {
      type: 'string',
      short: 'o',
      description: 'Output directory',
      default: './data',
    },
  },
  examples: `
# Fetch all indices
${CLI_NAME} jquants fetch indices

# Fetch specific index
${CLI_NAME} jquants fetch indices -c 0000

# Fetch with date range
${CLI_NAME} jquants fetch indices -f 2024-01-01 -t 2024-12-31

# Export as JSON
${CLI_NAME} jquants fetch indices --json
  `.trim(),
  run: async (ctx) => {
    const { code, date, from, to, json, output } = ctx.values;
    await fetchIndices({ code, date, from, to, json, output: output || './data' });
  },
});
