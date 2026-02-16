/**
 * Portfolio Factor Regression Command
 * Analyze portfolio risk factors via OLS regression
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';
import { ApiClient, type IndexMatch, type PortfolioFactorRegressionResponse } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { CLIError, CLIValidationError } from '../../utils/error-handling.js';

/**
 * Execute portfolio factor regression analysis via API
 */
async function executePortfolioFactorRegression(options: {
  portfolioId: string;
  lookbackDays?: string;
  format?: string;
  debug?: boolean;
}): Promise<void> {
  const apiClient = new ApiClient();
  const lookbackDays = options.lookbackDays ? Number.parseInt(options.lookbackDays, 10) : 252;

  // Resolve portfolio ID (could be name or ID)
  let portfolioId: number;

  // Check if it's a number
  const parsedId = Number.parseInt(options.portfolioId, 10);
  if (!Number.isNaN(parsedId) && parsedId > 0) {
    portfolioId = parsedId;
  } else {
    // It's a name, look up the ID
    if (options.debug) {
      console.log(chalk.gray(`Looking up portfolio by name: ${options.portfolioId}`));
    }

    const { portfolios } = await apiClient.portfolio.listPortfolios();
    const found = portfolios.find((p) => p.name.toLowerCase() === options.portfolioId.toLowerCase());

    if (!found) {
      throw new Error(`Portfolio not found: ${options.portfolioId}`);
    }

    portfolioId = found.id;
  }

  if (options.debug) {
    console.log(chalk.gray('Using API endpoint for portfolio factor regression analysis'));
    console.log(chalk.gray(`Portfolio ID: ${portfolioId}`));
    console.log(chalk.gray(`Lookback Days: ${lookbackDays}`));
  }

  const spinner = ora('Analyzing portfolio risk factors via API...').start();

  try {
    const response = await apiClient.analytics.getPortfolioFactorRegression({
      portfolioId,
      lookbackDays,
    });

    spinner.succeed(chalk.green('Portfolio factor regression analysis complete'));

    if (options.format === 'json') {
      console.log(JSON.stringify(response, null, 2));
    } else {
      printPortfolioFactorRegressionTable(response);
    }
  } catch (error) {
    spinner.fail(chalk.red('Analysis failed'));
    throw error;
  }
}

/**
 * Print portfolio factor regression results as formatted table
 */
function printPortfolioFactorRegressionTable(result: PortfolioFactorRegressionResponse): void {
  console.log(chalk.cyan('\n=== Portfolio Factor Regression Analysis ==='));
  console.log(chalk.yellow(`Portfolio: ${result.portfolioName} (ID: ${result.portfolioId})`));
  console.log(chalk.gray(`Period: ${result.dateRange.from} ~ ${result.dateRange.to}`));
  console.log(chalk.gray(`Data Points: ${result.dataPoints} trading days`));

  // Portfolio composition
  console.log(chalk.cyan('\n--- Portfolio Composition ---'));
  console.log(`  Total Value: ${result.totalValue.toLocaleString()} 円`);
  console.log(`  Stocks: ${result.stockCount} (${result.includedStockCount} included in analysis)`);

  // Weight table
  console.log(chalk.yellow('\n  Weights by Current Market Value:'));
  const sortedWeights = [...result.weights].sort((a, b) => b.weight - a.weight);
  for (const w of sortedWeights) {
    const weightPct = (w.weight * 100).toFixed(1);
    console.log(
      chalk.gray(`    ${w.code} ${w.companyName.padEnd(20)} ${weightPct.padStart(5)}%  `) +
        chalk.gray(
          `${w.quantity.toLocaleString().padStart(8)} 株 × ${w.latestPrice.toLocaleString().padStart(8)} 円 = ${w.marketValue.toLocaleString().padStart(12)} 円`
        )
    );
  }

  // Excluded stocks warning
  if (result.excludedStocks.length > 0) {
    console.log(chalk.yellow('\n  Excluded Stocks (insufficient data):'));
    for (const s of result.excludedStocks) {
      console.log(chalk.gray(`    ${s.code} ${s.companyName}: ${s.reason}`));
    }
  }

  // Stage 1: Market
  console.log(chalk.cyan('\n--- Stage 1: Market Regression (vs TOPIX) ---'));
  console.log(`  Market Beta (βm): ${result.marketBeta.toFixed(3)}`);
  console.log(`  Market R²: ${(result.marketRSquared * 100).toFixed(1)}%`);

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
    console.log(chalk.gray(`     R² = ${rSquaredPct}%, β = ${betaStr}`));
  }
}

/**
 * Handle portfolio factor regression errors
 */
function handlePortfolioFactorRegressionError(error: unknown): void {
  console.error(chalk.red('\nPortfolio factor regression analysis failed:'));

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
  console.error(chalk.gray(`   Ensure the portfolio exists: ${CLI_NAME} portfolio list`));
  console.error(chalk.gray('   Try with --debug flag for more information'));
}

/**
 * Portfolio factor regression command definition
 */
export const portfolioFactorRegressionCommand = define({
  name: 'portfolio-factor-regression',
  description: 'Analyze portfolio risk factors via OLS regression',
  args: {
    portfolioId: {
      type: 'positional',
      description: 'Portfolio ID or name',
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
# Basic analysis by portfolio ID (1 year, 252 trading days)
${CLI_NAME} analysis portfolio-factor-regression 1

# Basic analysis by portfolio name
${CLI_NAME} analysis portfolio-factor-regression "My Portfolio"

# Custom lookback period (6 months)
${CLI_NAME} analysis portfolio-factor-regression 1 --lookback-days 126

# JSON output
${CLI_NAME} analysis portfolio-factor-regression 1 --format json

# With debug output
${CLI_NAME} analysis portfolio-factor-regression 1 --debug
  `.trim(),
  run: async (ctx) => {
    const { portfolioId, lookbackDays, format, debug } = ctx.values;

    if (!portfolioId) {
      throw new CLIValidationError(
        `Portfolio ID or name is required\nUsage: ${CLI_NAME} analysis portfolio-factor-regression <portfolioId>`
      );
    }

    try {
      await executePortfolioFactorRegression({
        portfolioId,
        lookbackDays,
        format,
        debug,
      });
    } catch (error: unknown) {
      handlePortfolioFactorRegressionError(error);
      throw new CLIError('Portfolio factor regression failed', 1, true, { cause: error });
    }
  },
});
