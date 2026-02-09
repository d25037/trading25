/**
 * Portfolio List Command
 * List all portfolios via API
 */

import type { PortfolioSummaryResponse } from '@trading25/portfolio-db-ts/portfolio';
import chalk from 'chalk';
import { define } from 'gunshi';
import { ApiClient } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { CLIError } from '../../utils/error-handling.js';

/**
 * Display portfolio list
 */
function displayPortfolioList(portfolios: PortfolioSummaryResponse[], detailed: boolean): void {
  console.log(chalk.bold.cyan('\nPortfolios'));
  console.log(chalk.bold('='.repeat(80)));

  for (const portfolio of portfolios) {
    console.log(chalk.white(`\n${chalk.bold(portfolio.name)} ${chalk.gray(`(ID: ${portfolio.id})`)}`));

    if (portfolio.description) {
      console.log(chalk.gray(`  ${portfolio.description}`));
    }

    if (detailed) {
      console.log(chalk.gray(`  Stocks: ${portfolio.stockCount}`));
      console.log(chalk.gray(`  Total Shares: ${portfolio.totalShares.toLocaleString()}`));
    }

    const createdAt = new Date(portfolio.createdAt);
    console.log(chalk.gray(`  Created: ${createdAt.toLocaleString('ja-JP')}`));
  }

  console.log(chalk.bold(`\n${'='.repeat(80)}`));
  console.log(chalk.gray(`Total: ${portfolios.length} portfolio${portfolios.length > 1 ? 's' : ''}`));
}

/**
 * List command definition
 */
export const listCommand = define({
  name: 'list',
  description: 'List all portfolios',
  args: {
    detailed: {
      type: 'boolean',
      description: 'Show detailed statistics for each portfolio',
    },
    debug: {
      type: 'boolean',
      description: 'Enable debug logging',
    },
  },
  examples: `
# List all portfolios
${CLI_NAME} portfolio list

# Show detailed statistics
${CLI_NAME} portfolio list --detailed
  `.trim(),
  run: async (ctx) => {
    const { detailed, debug } = ctx.values;

    try {
      const apiClient = new ApiClient();

      if (debug) {
        console.log(chalk.gray('[DEBUG] Fetching portfolios from API'));
      }

      // Get portfolios from API
      const response = await apiClient.listPortfolios();
      const portfolios = response.portfolios;

      if (portfolios.length === 0) {
        console.log(chalk.yellow('No portfolios found.'));
        console.log(chalk.gray('\nCreate a portfolio with: portfolio create <name>'));
        return;
      }

      // Display portfolios
      displayPortfolioList(portfolios, detailed ?? false);
    } catch (error) {
      if (error instanceof CLIError) throw error;
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error(chalk.red(`✗ Failed to list portfolios: ${errorMessage}`));

      if (debug && error instanceof Error && error.stack) {
        console.error(chalk.gray(`\n[DEBUG] Stack trace:\n${error.stack}`));
      }

      throw new CLIError(errorMessage, 1, true, { cause: error });
    }
  },
});
