/**
 * Portfolio Delete Command
 * Delete a portfolio and all its holdings via API
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import { ApiClient } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { CLICancelError, CLIError, CLINotFoundError, CLIValidationError } from '../../utils/error-handling.js';

/**
 * Find portfolio by name or ID
 */
async function findPortfolio(apiClient: ApiClient, nameOrId: string) {
  const portfolioId = Number.parseInt(nameOrId, 10);

  if (Number.isNaN(portfolioId)) {
    // Search by name
    const response = await apiClient.portfolio.listPortfolios();
    const found = response.portfolios.find((p: { name: string }) => p.name === nameOrId);

    if (!found) {
      throw new CLINotFoundError(`Portfolio not found: ${nameOrId}. List all portfolios with: portfolio list`);
    }

    return apiClient.portfolio.getPortfolio(found.id);
  }

  // Get by ID
  return apiClient.portfolio.getPortfolio(portfolioId);
}

/**
 * Show delete warning message
 */
function showDeleteWarning(portfolioName: string, itemCount: number): void {
  console.log(chalk.yellow(`\n⚠️  Warning: This will delete portfolio "${portfolioName}"`));

  if (itemCount > 0) {
    console.log(chalk.yellow(`⚠️  This portfolio contains ${itemCount} stock holding${itemCount > 1 ? 's' : ''}`));
    console.log(chalk.yellow('⚠️  All holdings will be permanently deleted'));
  }

  console.log(chalk.gray('\nThis action cannot be undone.'));
  console.log(chalk.gray('\nTo proceed, run this command again with --force flag:'));
  console.log(chalk.gray(`  portfolio delete "${portfolioName}" --force`));
}

/**
 * Delete portfolio and show feedback
 */
async function deletePortfolioWithFeedback(
  apiClient: ApiClient,
  portfolio: Awaited<ReturnType<typeof apiClient.portfolio.getPortfolio>>,
  itemCount: number,
  debug: boolean
): Promise<void> {
  if (debug) {
    console.log(chalk.gray('[DEBUG] Deleting portfolio via API'));
  }

  await apiClient.portfolio.deletePortfolio(portfolio.id);

  console.log(chalk.green(`✓ Deleted portfolio: ${chalk.bold(portfolio.name)}`));
  if (itemCount > 0) {
    console.log(chalk.gray(`  ${itemCount} stock holding${itemCount > 1 ? 's' : ''} removed`));
  }
}

/**
 * Handle delete error
 */
function handleDeleteError(error: unknown, debug: boolean): never {
  if (error instanceof CLIError) throw error;
  const errorMessage = error instanceof Error ? error.message : String(error);
  console.error(chalk.red(`✗ Failed to delete portfolio: ${errorMessage}`));

  if (debug && error instanceof Error && error.stack) {
    console.error(chalk.gray(`\n[DEBUG] Stack trace:\n${error.stack}`));
  }

  throw new CLIError(errorMessage, 1, true, { cause: error });
}

/**
 * Delete command definition
 */
export const deleteCommand = define({
  name: 'delete',
  description: 'Delete a portfolio and all its holdings',
  args: {
    nameOrId: {
      type: 'positional',
      description: 'Portfolio name or ID',
    },
    force: {
      type: 'boolean',
      description: 'Skip confirmation and delete immediately',
    },
    debug: {
      type: 'boolean',
      description: 'Enable debug logging',
    },
  },
  examples: `
# Delete with confirmation prompt
${CLI_NAME} portfolio delete "My Portfolio"

# Force delete without confirmation
${CLI_NAME} portfolio delete "My Portfolio" --force
  `.trim(),
  run: async (ctx) => {
    const { nameOrId, force, debug } = ctx.values;

    if (!nameOrId) {
      throw new CLIValidationError(
        `Portfolio name or ID is required\nUsage: ${CLI_NAME} portfolio delete <name-or-id>`
      );
    }

    try {
      const apiClient = new ApiClient();

      if (debug) {
        console.log(chalk.gray('[DEBUG] Fetching portfolio from API'));
      }

      const portfolio = await findPortfolio(apiClient, nameOrId);
      const itemCount = portfolio.items.length;

      if (!force) {
        showDeleteWarning(portfolio.name, itemCount);
        throw new CLICancelError();
      }

      await deletePortfolioWithFeedback(apiClient, portfolio, itemCount, debug ?? false);
    } catch (error) {
      handleDeleteError(error, debug ?? false);
    }
  },
});
