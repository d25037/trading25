import chalk from 'chalk';
import { define } from 'gunshi';
import { ApiClient } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { CLICancelError, CLIError, CLIValidationError } from '../../utils/error-handling.js';
import { resolveWatchlist } from './resolve-watchlist.js';

function showDeleteConfirmation(watchlistName: string, itemCount: number): void {
  console.log(chalk.yellow(`\n⚠️  Warning: This will delete watchlist "${watchlistName}"`));
  if (itemCount > 0) {
    console.log(chalk.yellow(`⚠️  This watchlist contains ${itemCount} stock${itemCount > 1 ? 's' : ''}`));
  }
  console.log(chalk.gray('\nThis action cannot be undone.'));
  console.log(chalk.gray('\nTo proceed, run this command again with --force flag:'));
  console.log(chalk.gray(`  watchlist delete "${watchlistName}" --force`));
}

export const deleteCommand = define({
  name: 'delete',
  description: 'Delete a watchlist and all its items',
  args: {
    nameOrId: {
      type: 'positional',
      description: 'Watchlist name or ID',
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
${CLI_NAME} watchlist delete "Tech Stocks"

# Force delete without confirmation
${CLI_NAME} watchlist delete "Tech Stocks" --force
  `.trim(),
  run: async (ctx) => {
    const { nameOrId, force, debug } = ctx.values;

    if (!nameOrId) {
      throw new CLIValidationError(
        `Watchlist name or ID is required\nUsage: ${CLI_NAME} watchlist delete <name-or-id>`
      );
    }

    try {
      const apiClient = new ApiClient();

      if (debug) {
        console.log(chalk.gray('[DEBUG] Fetching watchlist from API'));
      }

      const watchlist = await resolveWatchlist(apiClient, nameOrId);
      const itemCount = watchlist.items.length;

      if (!force) {
        showDeleteConfirmation(watchlist.name, itemCount);
        throw new CLICancelError();
      }

      if (debug) {
        console.log(chalk.gray('[DEBUG] Deleting watchlist via API'));
      }

      await apiClient.watchlist.deleteWatchlist(watchlist.id);

      console.log(chalk.green(`✓ Deleted watchlist: ${chalk.bold(watchlist.name)}`));
      if (itemCount > 0) {
        console.log(chalk.gray(`  ${itemCount} stock${itemCount > 1 ? 's' : ''} removed`));
      }
    } catch (error) {
      if (error instanceof CLIError) throw error;
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error(chalk.red(`✗ Failed to delete watchlist: ${errorMessage}`));

      if (debug && error instanceof Error && error.stack) {
        console.error(chalk.gray(`\n[DEBUG] Stack trace:\n${error.stack}`));
      }

      throw new CLIError(errorMessage, 1, true, { cause: error });
    }
  },
});
