/**
 * ROE Analysis Command
 * Calculate Return on Equity from JQuants financial statements data
 */

import { define } from 'gunshi';
import { CLI_NAME } from '../../utils/constants.js';
import { CLIError } from '../../utils/error-handling.js';
import { executeROEAnalysis, handleROEAnalysisError } from './roe-helper.js';
export const roeCommand = define({
  name: 'roe',
  description: 'Calculate Return on Equity (ROE) from financial statements',
  args: {
    code: {
      type: 'string',
      short: 'c',
      description: 'Stock codes (comma-separated for multiple stocks)',
    },
    date: {
      type: 'string',
      short: 'd',
      description: 'Specific date (YYYYMMDD or YYYY-MM-DD)',
    },
    annualize: {
      type: 'boolean',
      description: 'Annualize quarterly data (default: true)',
      default: true,
    },
    preferNonConsolidated: {
      type: 'boolean',
      description: 'Prefer non-consolidated over consolidated data',
    },
    minEquity: {
      type: 'string',
      description: 'Minimum equity threshold',
      default: '1000',
    },
    sortBy: {
      type: 'string',
      description: 'Sort results by field (roe|code|date)',
      default: 'roe',
    },
    format: {
      type: 'string',
      description: 'Output format (table|json|csv)',
      default: 'table',
    },
    limit: {
      type: 'string',
      description: 'Limit number of results',
      default: '50',
    },
    debug: {
      type: 'boolean',
      description: 'Enable debug output',
    },
    verbose: {
      type: 'boolean',
      description: 'Enable verbose output',
    },
  },
  examples: `
# ROE analysis for specific stocks
${CLI_NAME} analysis roe --code 7203
${CLI_NAME} analysis roe -c 7203,6758,9984

# ROE analysis for a specific date
${CLI_NAME} analysis roe --date 2025-01-01

# With formatting options
${CLI_NAME} analysis roe -c 7203 --format json --sort-by roe

# Non-consolidated statements only
${CLI_NAME} analysis roe -c 7203 --prefer-non-consolidated

# Limit results
${CLI_NAME} analysis roe --date 2025-01-01 --limit 20
  `.trim(),
  run: async (ctx) => {
    const { code, date, annualize, preferNonConsolidated, minEquity, sortBy, format, limit, debug, verbose } =
      ctx.values;

    try {
      await executeROEAnalysis({
        code,
        date,
        annualize,
        preferConsolidated: !preferNonConsolidated,
        minEquity,
        sortBy: sortBy as 'roe' | 'code' | 'date',
        format: format as 'table' | 'json' | 'csv',
        limit,
        debug,
        verbose,
      });
    } catch (error: unknown) {
      handleROEAnalysisError(error);
      throw new CLIError('ROE analysis failed', 1, true, { cause: error });
    }
  },
});
