/**
 * ROE Analysis Helper Functions
 * Uses API endpoint for ROE calculations
 */

import { ApiClient, type ROEResultItem } from '../../utils/api-client';
import { OutputManager } from '../../utils/OutputManager';
import { outputCSV, outputTable } from './roe-output';

interface ROEOptions {
  code?: string;
  date?: string;
  annualize?: boolean;
  preferConsolidated?: boolean;
  minEquity?: string;
  sortBy?: 'roe' | 'code' | 'date';
  format?: 'table' | 'json' | 'csv';
  limit?: string;
  debug?: boolean;
  verbose?: boolean;
}

/**
 * Main ROE analysis execution function
 */
export async function executeROEAnalysis(options: ROEOptions): Promise<void> {
  const output = new OutputManager(options.debug ? 'debug' : 'production');

  output.info('ROE Analysis: Calculating Return on Equity from financial statements');

  // Validate that either code or date is provided
  if (!options.code && !options.date) {
    output.error('Either --code or --date parameter is required');
    throw new Error('Missing required parameters');
  }

  output.info('Fetching financial statements via API...');
  if (options.code) {
    output.debug(`Stock codes: ${options.code}`);
  }
  if (options.date) {
    output.debug(`Date: ${options.date}`);
  }

  const apiClient = new ApiClient();

  try {
    const response = await apiClient.getROE({
      code: options.code,
      date: options.date,
      annualize: options.annualize !== false,
      preferConsolidated: options.preferConsolidated !== false,
      minEquity: options.minEquity ? Number.parseInt(options.minEquity, 10) : undefined,
      sortBy: options.sortBy || 'roe',
      limit: options.limit ? Number.parseInt(options.limit, 10) : undefined,
    });

    if (response.results.length === 0) {
      showNoResultsWarning(output);
      return;
    }

    output.info(`Found ${response.summary.totalCompanies} companies with ROE data`);

    // Convert API response to legacy format for output functions
    const results = response.results.map((item) => ({
      roe: item.roe,
      netProfit: item.netProfit,
      equity: item.equity,
      metadata: {
        code: item.metadata.code,
        periodType: item.metadata.periodType,
        periodEnd: item.metadata.periodEnd,
        isConsolidated: item.metadata.isConsolidated,
        accountingStandard: item.metadata.accountingStandard,
        isAnnualized: item.metadata.isAnnualized,
      },
    }));

    outputResults(results, options.format, output);
    output.success(`ROE analysis completed for ${results.length} companies`);
  } catch (error) {
    if (error instanceof Error && error.message.includes('Cannot connect to API server')) {
      output.error('Cannot connect to API server.');
      output.info('Please ensure the API server is running with "uv run bt server --port 3002"');
      throw error;
    }
    throw error;
  }
}

/**
 * Handle ROE analysis errors
 */
export function handleROEAnalysisError(error: unknown): void {
  const output = new OutputManager('production');

  const errorMessage = error instanceof Error ? error.message : String(error);
  output.error(`ROE analysis failed: ${errorMessage}`);
}

/**
 * Show warning when no results are found
 */
function showNoResultsWarning(output: OutputManager): void {
  output.warn('No ROE calculations possible with the given criteria');
  output.info('This could be due to:');
  output.info('  - Missing profit or equity data');
  output.info('  - Equity values below minimum threshold');
  output.info('  - Invalid or zero equity values');
}

/**
 * Output results in specified format
 */
function outputResults(results: ROEResultItem[], format?: string, output?: OutputManager): void {
  switch (format || 'table') {
    case 'json':
      console.log(JSON.stringify(results, null, 2));
      break;
    case 'csv':
      outputCSV(results);
      break;
    default:
      if (output) {
        outputTable(results, output);
      }
      break;
  }
}
