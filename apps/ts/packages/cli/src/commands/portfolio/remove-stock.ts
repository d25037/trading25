/**
 * Portfolio Remove Stock Command
 * Remove a stock from a portfolio via API
 */

import type { PortfolioItemResponse } from '@trading25/shared/portfolio';
import chalk from 'chalk';
import { define } from 'gunshi';
import { ApiClient } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { CLIError, CLIValidationError } from '../../utils/error-handling.js';

/**
 * Display removed stock details
 */
function displayStockRemoved(item: PortfolioItemResponse): void {
  console.log(chalk.gray(`  Quantity: ${item.quantity.toLocaleString()} shares`));
  console.log(chalk.gray(`  Purchase price: ¥${item.purchasePrice.toLocaleString()}`));
  console.log(chalk.gray(`  Purchase date: ${item.purchaseDate}`));
  console.log(chalk.gray(`  Total cost: ¥${(item.quantity * item.purchasePrice).toLocaleString()}`));

  if (item.account) {
    console.log(chalk.gray(`  Account: ${item.account}`));
  }

  if (item.notes) {
    console.log(chalk.gray(`  Notes: ${item.notes}`));
  }
}

/**
 * Remove stock command definition
 */
export const removeStockCommand = define({
  name: 'remove-stock',
  description: 'Remove a stock from a portfolio',
  args: {
    portfolio: {
      type: 'positional',
      description: 'Portfolio name',
    },
    code: {
      type: 'positional',
      description: 'Stock code (e.g., 7203)',
    },
    debug: {
      type: 'boolean',
      description: 'Enable debug logging',
    },
  },
  examples: `
# Remove stock from portfolio
${CLI_NAME} portfolio remove-stock "My Portfolio" 7203

# Remove with debug output
${CLI_NAME} portfolio remove-stock "Growth" 6758 --debug
  `.trim(),
  run: async (ctx) => {
    const { portfolio: portfolioName, code, debug } = ctx.values;

    if (!portfolioName || !code) {
      throw new CLIValidationError(
        `Portfolio name and stock code are required\nUsage: ${CLI_NAME} portfolio remove-stock <portfolio> <code>`
      );
    }

    try {
      const apiClient = new ApiClient();

      if (debug) {
        console.log(chalk.gray(`[DEBUG] Removing stock ${code} from portfolio "${portfolioName}"`));
      }

      // Delete stock via API
      const response = await apiClient.deletePortfolioStock(portfolioName, code);

      // Success message
      console.log(
        chalk.green(
          `✓ Removed ${chalk.bold(response.deletedItem.companyName)} (${response.deletedItem.code}) from "${portfolioName}"`
        )
      );

      // Display item details
      displayStockRemoved(response.deletedItem);
    } catch (error) {
      if (error instanceof CLIError) throw error;
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error(chalk.red(`✗ Failed to remove stock: ${errorMessage}`));

      if (debug && error instanceof Error && error.stack) {
        console.error(chalk.gray(`\n[DEBUG] Stack trace:\n${error.stack}`));
      }

      throw new CLIError(errorMessage, 1, true, { cause: error });
    }
  },
});
