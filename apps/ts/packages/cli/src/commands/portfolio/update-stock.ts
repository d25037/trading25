/**
 * Portfolio Update Stock Command
 * Update a stock holding in a portfolio via API
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import { ApiClient } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { CLICancelError, CLIError, CLIValidationError } from '../../utils/error-handling.js';

interface ParsedUpdateInputs {
  quantity?: number;
  purchasePrice?: number;
  purchaseDate?: string;
  account?: string;
  notes?: string;
}

/**
 * Parse and validate update inputs
 */
function parseUpdateInputs(
  quantity?: string,
  price?: string,
  date?: string,
  account?: string,
  notes?: string
): ParsedUpdateInputs {
  const updateInput: ParsedUpdateInputs = {};

  if (quantity !== undefined) {
    const qty = Number.parseInt(quantity, 10);
    if (Number.isNaN(qty) || qty <= 0) {
      throw new CLIValidationError('Invalid quantity. Must be a positive number.');
    }
    updateInput.quantity = qty;
  }

  if (price !== undefined) {
    const p = Number.parseFloat(price);
    if (Number.isNaN(p) || p <= 0) {
      throw new CLIValidationError('Invalid price. Must be a positive number.');
    }
    updateInput.purchasePrice = p;
  }

  if (date !== undefined) {
    const dateObj = new Date(date);
    if (Number.isNaN(dateObj.getTime())) {
      throw new CLIValidationError('Invalid date format. Use YYYY-MM-DD');
    }
    updateInput.purchaseDate = date;
  }

  if (account !== undefined) {
    updateInput.account = account;
  }

  if (notes !== undefined) {
    updateInput.notes = notes;
  }

  return updateInput;
}

/**
 * Display updated stock details
 */
function displayUpdatedStock(item: {
  code: string;
  companyName: string;
  quantity: number;
  purchasePrice: number;
  purchaseDate: string;
  account?: string;
  notes?: string;
}): void {
  console.log(chalk.green(`✓ Updated ${chalk.bold(item.companyName)} (${item.code})`));
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
 * Update stock command definition
 */
export const updateStockCommand = define({
  name: 'update-stock',
  description: 'Update a stock holding in a portfolio',
  args: {
    portfolio: {
      type: 'positional',
      description: 'Portfolio name',
    },
    code: {
      type: 'positional',
      description: 'Stock code (e.g., 7203)',
    },
    quantity: {
      type: 'string',
      short: 'q',
      description: 'New quantity',
    },
    price: {
      type: 'string',
      short: 'p',
      description: 'New purchase price',
    },
    date: {
      type: 'string',
      short: 'd',
      description: 'New purchase date (YYYY-MM-DD)',
    },
    account: {
      type: 'string',
      short: 'a',
      description: 'New account name',
    },
    notes: {
      type: 'string',
      short: 'n',
      description: 'New notes',
    },
    debug: {
      type: 'boolean',
      description: 'Enable debug logging',
    },
  },
  examples: `
# Update quantity
${CLI_NAME} portfolio update-stock "My Portfolio" 7203 -q 150

# Update multiple fields
${CLI_NAME} portfolio update-stock "Growth" 6758 -q 200 -p 2600 -a "NISA"
  `.trim(),
  run: async (ctx) => {
    const { portfolio: portfolioName, code, quantity, price, date, account, notes, debug } = ctx.values;

    if (!portfolioName || !code) {
      throw new CLIValidationError(
        `Portfolio name and stock code are required\nUsage: ${CLI_NAME} portfolio update-stock <portfolio> <code> [options]`
      );
    }

    try {
      const apiClient = new ApiClient();

      // Build update input
      const updateInput = parseUpdateInputs(quantity, price, date, account, notes);

      // Check if any updates provided
      if (Object.keys(updateInput).length === 0) {
        console.log(chalk.yellow('No updates provided.'));
        console.log(chalk.gray('\nUse --quantity, --price, --date, --account, or --notes to update'));
        throw new CLICancelError();
      }

      if (debug) {
        console.log(chalk.gray(`[DEBUG] Updating stock ${code} in portfolio "${portfolioName}"`));
      }

      // Update stock via API
      const updatedItem = await apiClient.updatePortfolioStock(portfolioName, code, updateInput);

      // Display success message
      displayUpdatedStock(updatedItem);
    } catch (error) {
      if (error instanceof CLIError) throw error;
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error(chalk.red(`✗ Failed to update stock: ${errorMessage}`));

      if (debug && error instanceof Error && error.stack) {
        console.error(chalk.gray(`\n[DEBUG] Stack trace:\n${error.stack}`));
      }

      throw new CLIError(errorMessage, 1, true, { cause: error });
    }
  },
});
