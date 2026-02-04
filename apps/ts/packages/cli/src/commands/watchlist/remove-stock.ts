import chalk from 'chalk';
import { define } from 'gunshi';
import { ApiClient } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { CLIError, CLINotFoundError, CLIValidationError } from '../../utils/error-handling.js';
import { resolveWatchlistId } from './resolve-watchlist.js';

export const removeStockCommand = define({
  name: 'remove-stock',
  description: 'Remove a stock from a watchlist',
  args: {
    watchlist: {
      type: 'positional',
      description: 'Watchlist name or ID',
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
# Remove stock from watchlist
${CLI_NAME} watchlist remove-stock "Tech Stocks" 7203
  `.trim(),
  run: async (ctx) => {
    const { watchlist: watchlistNameOrId, code, debug } = ctx.values;

    if (!watchlistNameOrId || !code) {
      throw new CLIValidationError(
        `Watchlist name/ID and stock code are required\nUsage: ${CLI_NAME} watchlist remove-stock <watchlist> <code>`
      );
    }

    try {
      const apiClient = new ApiClient();

      if (debug) {
        console.log(chalk.gray(`[DEBUG] Removing stock ${code} from watchlist "${watchlistNameOrId}"`));
      }

      const resolvedId = await resolveWatchlistId(apiClient, watchlistNameOrId);
      const watchlist = await apiClient.getWatchlist(resolvedId);
      const item = watchlist.items.find((i: { code: string }) => i.code === code);

      if (!item) {
        throw new CLINotFoundError(`Stock ${code} not found in watchlist "${watchlistNameOrId}"`);
      }

      // Delete item
      await apiClient.deleteWatchlistItem(resolvedId, item.id);

      console.log(
        chalk.green(`✓ Removed ${chalk.bold(item.companyName)} (${item.code}) from watchlist "${watchlistNameOrId}"`)
      );
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
