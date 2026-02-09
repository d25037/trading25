/**
 * Portfolio Show Command
 * Show portfolio details with all holdings via API
 */

import type { PortfolioItemResponse, PortfolioWithItemsResponse } from '@trading25/portfolio-db-ts/portfolio';
import chalk from 'chalk';
import { define } from 'gunshi';
import { ApiClient } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { CLIError, CLINotFoundError, CLIValidationError } from '../../utils/error-handling.js';

/**
 * Display portfolio header information
 */
function displayPortfolioHeader(portfolio: PortfolioWithItemsResponse): void {
  console.log(chalk.bold.cyan(`\n${portfolio.name}`));
  console.log(chalk.bold('='.repeat(80)));

  if (portfolio.description) {
    console.log(chalk.white(portfolio.description));
    console.log();
  }

  console.log(chalk.gray(`ID: ${portfolio.id}`));
  const createdAt = new Date(portfolio.createdAt);
  const updatedAt = new Date(portfolio.updatedAt);
  console.log(chalk.gray(`Created: ${createdAt.toLocaleString('ja-JP')}`));
  console.log(chalk.gray(`Updated: ${updatedAt.toLocaleString('ja-JP')}`));
}

/**
 * Display holdings table
 */
function displayHoldingsTable(portfolio: PortfolioWithItemsResponse): void {
  console.log(chalk.bold('\n\nHoldings:'));
  console.log(chalk.bold('-'.repeat(80)));

  // Table header
  const codeHeader = 'Code'.padEnd(8);
  const companyHeader = 'Company'.padEnd(25);
  const quantityHeader = 'Quantity'.padStart(10);
  const priceHeader = 'Price'.padStart(12);
  const dateHeader = 'Purchase Date'.padEnd(15);
  const accountHeader = 'Account'.padEnd(15);

  console.log(
    chalk.bold.white(`${codeHeader}${companyHeader}${quantityHeader}${priceHeader}${dateHeader}${accountHeader}`)
  );
  console.log(chalk.gray('-'.repeat(80)));

  // Table rows
  for (const item of portfolio.items) {
    const code = chalk.cyan(item.code.padEnd(8));
    const company = item.companyName.substring(0, 23).padEnd(25);
    const quantity = chalk.yellow(item.quantity.toLocaleString().padStart(10));
    const price = chalk.green(`¥${item.purchasePrice.toLocaleString()}`.padStart(12));
    const date = item.purchaseDate.padEnd(15);
    const account = (item.account ?? '-').substring(0, 13).padEnd(15);

    console.log(`${code}${company}${quantity}${price}${date}${account}`);

    if (item.notes) {
      console.log(chalk.gray(`        Note: ${item.notes}`));
    }
  }

  console.log(chalk.gray('-'.repeat(80)));
}

/**
 * Display holdings summary
 */
function displayHoldingsSummary(portfolio: PortfolioWithItemsResponse): void {
  const totalShares = portfolio.items.reduce((sum: number, item: PortfolioItemResponse) => sum + item.quantity, 0);
  const totalCost = portfolio.items.reduce(
    (sum: number, item: PortfolioItemResponse) => sum + item.quantity * item.purchasePrice,
    0
  );

  console.log(chalk.white(`Total Stocks: ${chalk.yellow(portfolio.items.length.toString())}`));
  console.log(chalk.white(`Total Shares: ${chalk.yellow(totalShares.toLocaleString())}`));
  console.log(chalk.white(`Total Cost: ${chalk.green(`¥${totalCost.toLocaleString()}`)}`));
}

/**
 * Show command definition
 */
export const showCommand = define({
  name: 'show',
  description: 'Show portfolio details with all holdings',
  args: {
    nameOrId: {
      type: 'positional',
      description: 'Portfolio name or ID',
    },
    debug: {
      type: 'boolean',
      description: 'Enable debug logging',
    },
  },
  examples: `
# Show portfolio by name
${CLI_NAME} portfolio show "My Portfolio"

# Show portfolio by ID
${CLI_NAME} portfolio show 1
  `.trim(),
  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: CLI command with portfolio lookup, display, and error handling
  run: async (ctx) => {
    const { nameOrId, debug } = ctx.values;

    if (!nameOrId) {
      throw new CLIValidationError(`Portfolio name or ID is required\nUsage: ${CLI_NAME} portfolio show <name-or-id>`);
    }

    try {
      const apiClient = new ApiClient();

      if (debug) {
        console.log(chalk.gray('[DEBUG] Fetching portfolio from API'));
      }

      // Find portfolio (by ID or name)
      const portfolioId = Number.parseInt(nameOrId, 10);
      let portfolio: PortfolioWithItemsResponse;

      if (Number.isNaN(portfolioId)) {
        // Search by name
        const response = await apiClient.listPortfolios();
        const found = response.portfolios.find((p: { name: string }) => p.name === nameOrId);

        if (!found) {
          throw new CLINotFoundError(`Portfolio not found: ${nameOrId}. List all portfolios with: portfolio list`);
        }

        portfolio = await apiClient.getPortfolio(found.id);
      } else {
        // Get by ID
        portfolio = await apiClient.getPortfolio(portfolioId);
      }

      // Display portfolio information
      displayPortfolioHeader(portfolio);

      // Display holdings
      if (portfolio.items.length === 0) {
        console.log(chalk.yellow('\nNo holdings in this portfolio.'));
        console.log(chalk.gray('\nAdd stocks with: portfolio add-stock <portfolio> <code>'));
      } else {
        displayHoldingsTable(portfolio);
        displayHoldingsSummary(portfolio);
      }

      console.log(chalk.bold('='.repeat(80)));
    } catch (error) {
      if (error instanceof CLIError) throw error;
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error(chalk.red(`✗ Failed to show portfolio: ${errorMessage}`));

      if (debug && error instanceof Error && error.stack) {
        console.error(chalk.gray(`\n[DEBUG] Stack trace:\n${error.stack}`));
      }

      throw new CLIError(errorMessage, 1, true, { cause: error });
    }
  },
});
