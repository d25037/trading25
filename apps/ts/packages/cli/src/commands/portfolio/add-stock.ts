/**
 * Portfolio Add Stock Command
 * Add a stock to a portfolio via API
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';
import { ApiClient } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { CLINotFoundError, CLIValidationError, handleCommandError } from '../../utils/error-handling.js';

/**
 * Validate stock input parameters
 */
function validateStockInputs(
  quantity: string | undefined,
  price: string | undefined,
  date: string | undefined
): { quantity: number; purchasePrice: number; purchaseDate: string } {
  if (!quantity || !price || !date) {
    throw new CLIValidationError('Missing required options: --quantity, --price, and --date are required');
  }

  const qty = Number.parseInt(quantity, 10);
  const purchasePrice = Number.parseFloat(price);
  const purchaseDate = date;

  if (Number.isNaN(qty) || qty <= 0) {
    throw new CLIValidationError('Invalid quantity. Must be a positive number.');
  }

  if (Number.isNaN(purchasePrice) || purchasePrice <= 0) {
    throw new CLIValidationError('Invalid price. Must be a positive number.');
  }

  // Validate date format
  const dateObj = new Date(purchaseDate);
  if (Number.isNaN(dateObj.getTime())) {
    throw new CLIValidationError('Invalid date format. Use YYYY-MM-DD');
  }

  return { quantity: qty, purchasePrice, purchaseDate };
}

/**
 * Display added stock details
 */
function displayStockAdded(item: {
  code: string;
  companyName: string;
  quantity: number;
  purchasePrice: number;
  purchaseDate: string;
  account?: string;
  notes?: string;
}): void {
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
 * Add stock command definition
 */
export const addStockCommand = define({
  name: 'add-stock',
  description: 'Add a stock to a portfolio (company name fetched automatically from JQuants)',
  args: {
    portfolio: {
      type: 'positional',
      description: 'Portfolio name or ID',
    },
    code: {
      type: 'positional',
      description: 'Stock code (e.g., 7203)',
    },
    quantity: {
      type: 'string',
      short: 'q',
      description: 'Number of shares',
      required: true,
    },
    price: {
      type: 'string',
      short: 'p',
      description: 'Purchase price per share',
      required: true,
    },
    date: {
      type: 'string',
      short: 'd',
      description: 'Purchase date (YYYY-MM-DD)',
      required: true,
    },
    account: {
      type: 'string',
      short: 'a',
      description: 'Account name (e.g., SBI証券)',
    },
    notes: {
      type: 'string',
      short: 'n',
      description: 'Additional notes',
    },
    debug: {
      type: 'boolean',
      description: 'Enable debug logging',
    },
  },
  examples: `
# Add stock to portfolio
${CLI_NAME} portfolio add-stock "My Portfolio" 7203 -q 100 -p 2500 -d 2024-01-01

# Add with account and notes
${CLI_NAME} portfolio add-stock "Growth" 6758 -q 50 -p 15000 -d 2024-06-15 -a "NISA" -n "Sony Group"
  `.trim(),
  run: async (ctx) => {
    const { portfolio: portfolioNameOrId, code, quantity, price, date, account, notes, debug } = ctx.values;
    const spinner = ora('Initializing...').start();

    if (!portfolioNameOrId || !code) {
      spinner.fail(chalk.red('Portfolio name/ID and stock code are required'));
      throw new CLIValidationError(
        `Portfolio name/ID and stock code are required\nUsage: ${CLI_NAME} portfolio add-stock <portfolio> <code> -q <qty> -p <price> -d <date>`
      );
    }

    try {
      const apiClient = new ApiClient();

      // Find portfolio
      spinner.text = 'Finding portfolio...';
      const portfolioId = Number.parseInt(portfolioNameOrId, 10);
      let portfolio: { id: number };

      if (Number.isNaN(portfolioId)) {
        // Search by name
        const response = await apiClient.portfolio.listPortfolios();
        const found = response.portfolios.find((p: { name: string }) => p.name === portfolioNameOrId);

        if (!found) {
          throw new CLINotFoundError(
            `Portfolio not found: ${portfolioNameOrId}. List all portfolios with: portfolio list`
          );
        }

        portfolio = { id: found.id };
      } else {
        portfolio = { id: portfolioId };
      }

      // Parse and validate inputs
      const validated = validateStockInputs(quantity, price, date);

      // Add to portfolio via API (company name will be fetched by API)
      spinner.text = 'Adding stock to portfolio...';
      const item = await apiClient.portfolio.addPortfolioItem(portfolio.id, {
        code,
        quantity: validated.quantity,
        purchasePrice: validated.purchasePrice,
        purchaseDate: validated.purchaseDate,
        account,
        notes,
      });

      spinner.succeed(chalk.green(`✓ Added ${chalk.bold(item.companyName)} (${code}) to portfolio`));

      // Display item details
      displayStockAdded(item);
    } catch (error) {
      handleCommandError(error, spinner, {
        failMessage: 'Failed to add stock',
        debug,
      });
    }
  },
});
