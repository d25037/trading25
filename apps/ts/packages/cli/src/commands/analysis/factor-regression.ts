/**
 * Factor Regression Command
 * Analyze stock risk factors via OLS regression
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';
import { ApiClient } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { CLIError, CLIValidationError } from '../../utils/error-handling.js';

/**
 * Factor regression API response type
 */
interface FactorRegressionResponse {
  stockCode: string;
  companyName?: string;
  marketBeta: number;
  marketRSquared: number;
  sector17Matches: IndexMatch[];
  sector33Matches: IndexMatch[];
  topixStyleMatches: IndexMatch[];
  analysisDate: string;
  dataPoints: number;
  dateRange: {
    from: string;
    to: string;
  };
}

interface IndexMatch {
  indexCode: string;
  indexName: string;
  category: string;
  rSquared: number;
  beta: number;
}

/**
 * Execute factor regression analysis via API
 */
async function executeFactorRegression(options: {
  code: string;
  lookbackDays?: string;
  format?: string;
  debug?: boolean;
}): Promise<void> {
  const apiClient = new ApiClient();
  const lookbackDays = options.lookbackDays ? Number.parseInt(options.lookbackDays, 10) : 252;

  if (options.debug) {
    console.log(chalk.gray('Using API endpoint for factor regression analysis'));
    console.log(chalk.gray(`Stock: ${options.code}`));
    console.log(chalk.gray(`Lookback Days: ${lookbackDays}`));
  }

  const spinner = ora('Analyzing risk factors via API...').start();

  try {
    const response = await apiClient.analytics.getFactorRegression({
      symbol: options.code,
      lookbackDays,
    });

    spinner.succeed(chalk.green('Factor regression analysis complete'));

    if (options.format === 'json') {
      console.log(JSON.stringify(response, null, 2));
    } else {
      printFactorRegressionTable(response);
    }
  } catch (error) {
    spinner.fail(chalk.red('Analysis failed'));
    throw error;
  }
}

/**
 * Print factor regression results as formatted table
 */
function printFactorRegressionTable(result: FactorRegressionResponse): void {
  console.log(chalk.cyan('\n=== Factor Regression Analysis ==='));
  console.log(chalk.yellow(`Stock: ${result.stockCode}${result.companyName ? ` (${result.companyName})` : ''}`));
  console.log(chalk.gray(`Period: ${result.dateRange.from} ~ ${result.dateRange.to}`));
  console.log(chalk.gray(`Data Points: ${result.dataPoints} trading days`));

  // Stage 1: Market
  console.log(chalk.cyan('\n--- Stage 1: Market Regression (vs TOPIX) ---'));
  console.log(`  Market Beta (\u03B2m): ${result.marketBeta.toFixed(3)}`);
  console.log(`  Market R\u00B2: ${(result.marketRSquared * 100).toFixed(1)}%`);

  // Interpretation
  const betaInterpretation =
    result.marketBeta > 1.2
      ? chalk.red('High market sensitivity')
      : result.marketBeta > 0.8
        ? chalk.yellow('Moderate market sensitivity')
        : chalk.green('Low market sensitivity');
  console.log(`  Interpretation: ${betaInterpretation}`);

  // Stage 2: Factor Matches
  console.log(chalk.cyan('\n--- Stage 2: Residual Factor Matches ---'));

  console.log(chalk.yellow('\nTOPIX-17 Sectors (Top 3):'));
  printMatches(result.sector17Matches);

  console.log(chalk.yellow('\n33 Sectors (Top 3):'));
  printMatches(result.sector33Matches);

  console.log(chalk.yellow('\nTOPIX Size + Market + Style (Top 3):'));
  printMatches(result.topixStyleMatches);
}

/**
 * Print index matches
 */
function printMatches(matches: IndexMatch[]): void {
  if (matches.length === 0) {
    console.log(chalk.gray('  No significant matches found'));
    return;
  }

  for (let i = 0; i < matches.length; i++) {
    const match = matches[i];
    if (!match) continue;

    const rank = i + 1;
    const rSquaredPct = (match.rSquared * 100).toFixed(1);
    const betaStr = match.beta.toFixed(3);

    console.log(`  ${rank}. ${match.indexName} (${match.indexCode})`);
    console.log(chalk.gray(`     R\u00B2 = ${rSquaredPct}%, \u03B2 = ${betaStr}`));
  }
}

/**
 * Handle factor regression errors
 */
function handleFactorRegressionError(error: unknown): void {
  console.error(chalk.red('\nFactor regression analysis failed:'));

  if (error instanceof Error) {
    console.error(chalk.red(`   Error: ${error.message}`));

    if (process.env.DEBUG) {
      console.error(chalk.gray('   Stack trace:'));
      console.error(chalk.gray(error.stack));
    }
  } else {
    const errorString = String(error);
    console.error(chalk.red(`   ${errorString}`));
  }

  console.error(chalk.gray('\nTroubleshooting tips:'));
  console.error(chalk.gray('   Ensure the API server is running: uv run bt server --port 3002'));
  console.error(chalk.gray(`   Ensure market.db has indices data: ${CLI_NAME} db sync`));
  console.error(chalk.gray('   Try with --debug flag for more information'));
}

/**
 * Factor regression command definition
 */
export const factorRegressionCommand = define({
  name: 'factor-regression',
  description: 'Analyze stock risk factors via OLS regression',
  args: {
    code: {
      type: 'positional',
      description: 'Stock code (4-character)',
      required: true,
    },
    lookbackDays: {
      type: 'string',
      description: 'Number of trading days for analysis (default: 252)',
      default: '252',
    },
    format: {
      type: 'string',
      description: 'Output format (table|json)',
      default: 'table',
    },
    debug: {
      type: 'boolean',
      description: 'Enable debug output',
    },
  },
  examples: `
# Basic analysis (1 year, 252 trading days)
${CLI_NAME} analysis factor-regression 7203

# Custom lookback period (6 months)
${CLI_NAME} analysis factor-regression 7203 --lookback-days 126

# JSON output
${CLI_NAME} analysis factor-regression 7203 --format json

# With debug output
${CLI_NAME} analysis factor-regression 7203 --debug
  `.trim(),
  run: async (ctx) => {
    const { code, lookbackDays, format, debug } = ctx.values;

    if (!code) {
      throw new CLIValidationError(`Stock code is required\nUsage: ${CLI_NAME} analysis factor-regression <code>`);
    }

    try {
      await executeFactorRegression({
        code,
        lookbackDays,
        format,
        debug,
      });
    } catch (error: unknown) {
      handleFactorRegressionError(error);
      throw new CLIError('Factor regression failed', 1, true, { cause: error });
    }
  },
});
