/**
 * JQuants Fetch - Listed Info Command
 * Fetch listed stock information
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';
import { ApiClient } from '../../../utils/api-client.js';
import { CLI_NAME } from '../../../utils/constants.js';
import { CsvExporter } from '../../../utils/csv-exporter.js';
import { displayDataSummary } from '../helpers.js';
import { handleApiError } from '../index.js';
import { displayListedInfo } from './display.js';
import type { FetchOptions } from './types.js';
export async function fetchListedInfo(code: string | undefined, options: FetchOptions): Promise<void> {
  const spinner = ora('Fetching listed stock information...').start();

  try {
    const apiClient = new ApiClient();

    const params: { code?: string; date?: string } = {};
    if (code) params.code = code;
    if (options.date) params.date = options.date;

    const response = await apiClient.getListedInfo(params);

    if (response.info && response.info.length > 0) {
      spinner.succeed(chalk.green(`Fetched ${response.info.length} stock listings`));

      // Convert API response format to JQuants v2 format for display
      const infoForDisplay = response.info.map((item) => ({
        Code: item.code,
        CoName: item.companyName,
        CoNameEn: item.companyNameEnglish || '',
        Mkt: item.marketCode,
        MktNm: item.marketCodeName,
        S33: item.sector33Code,
        S33Nm: item.sector33CodeName,
        ScaleCat: item.scaleCategory,
      }));

      // Export to CSV/JSON
      const exportType = options.csv ? 'csv' : 'json';
      const exporter = new CsvExporter(options.output);

      if (exportType === 'csv') {
        const timestamp = new Date().toISOString().split('T')[0];
        const filename = `listed_info_${timestamp}.csv`;
        const filepath = await exporter.exportListedInfo(infoForDisplay, filename);
        console.log(chalk.green(`✅ CSV exported to: ${filepath}`));
      } else {
        const timestamp = new Date().toISOString().split('T')[0];
        const filename = `listed_info_${timestamp}.json`;
        const filepath = await exporter.exportJSON(infoForDisplay, filename);
        console.log(chalk.green(`✅ JSON exported to: ${filepath}`));
      }

      displayDataSummary(infoForDisplay, displayListedInfo);
    } else {
      spinner.warn(chalk.yellow('No data found'));
    }
  } catch (error) {
    spinner.fail();
    handleApiError(error, 'Failed to fetch listed info');
  }
}

/**
 * Listed info command definition
 */
export const listedInfoCommand = define({
  name: 'listed-info',
  description: 'Fetch listed stock information',
  args: {
    code: {
      type: 'positional',
      description: 'Stock code (optional)',
    },
    date: {
      type: 'string',
      short: 'd',
      description: 'Specific date (YYYY-MM-DD)',
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
# Fetch all listed stocks
${CLI_NAME} jquants fetch listed-info

# Fetch specific stock info
${CLI_NAME} jquants fetch listed-info 7203

# Export as CSV
${CLI_NAME} jquants fetch listed-info --csv
  `.trim(),
  run: async (ctx) => {
    const { code, date, csv, json, output } = ctx.values;
    await fetchListedInfo(code, { date, csv, json, output: output || './data' });
  },
});
