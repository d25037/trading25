/**
 * Watchlist List Command
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import { ApiClient } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { CLIError } from '../../utils/error-handling.js';

export const listCommand = define({
  name: 'list',
  description: 'List all watchlists',
  args: {
    debug: {
      type: 'boolean',
      description: 'Enable debug logging',
    },
  },
  examples: `
# List all watchlists
${CLI_NAME} watchlist list
  `.trim(),
  run: async (ctx) => {
    const { debug } = ctx.values;

    try {
      const apiClient = new ApiClient();

      if (debug) {
        console.log(chalk.gray('[DEBUG] Fetching watchlists from API'));
      }

      const response = await apiClient.watchlist.listWatchlists();
      const watchlists = response.watchlists;

      if (watchlists.length === 0) {
        console.log(chalk.yellow('No watchlists found.'));
        console.log(chalk.gray('\nCreate a watchlist with: watchlist create <name>'));
        return;
      }

      console.log(chalk.bold.cyan('\nWatchlists'));
      console.log(chalk.bold('='.repeat(60)));

      for (const watchlist of watchlists) {
        console.log(chalk.white(`\n${chalk.bold(watchlist.name)} ${chalk.gray(`(ID: ${watchlist.id})`)}`));

        if (watchlist.description) {
          console.log(chalk.gray(`  ${watchlist.description}`));
        }

        console.log(chalk.gray(`  Stocks: ${watchlist.stockCount}`));
        const createdAt = new Date(watchlist.createdAt);
        console.log(chalk.gray(`  Created: ${createdAt.toLocaleString('ja-JP')}`));
      }

      console.log(chalk.bold(`\n${'='.repeat(60)}`));
      console.log(chalk.gray(`Total: ${watchlists.length} watchlist${watchlists.length > 1 ? 's' : ''}`));
    } catch (error) {
      if (error instanceof CLIError) throw error;
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error(chalk.red(`✗ Failed to list watchlists: ${errorMessage}`));

      if (debug && error instanceof Error && error.stack) {
        console.error(chalk.gray(`\n[DEBUG] Stack trace:\n${error.stack}`));
      }

      throw new CLIError(errorMessage, 1, true, { cause: error });
    }
  },
});
