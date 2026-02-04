import type { WatchlistWithItemsResponse } from '@trading25/shared/watchlist';
import chalk from 'chalk';
import { define } from 'gunshi';
import { ApiClient } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { CLIError, CLIValidationError } from '../../utils/error-handling.js';
import { resolveWatchlist } from './resolve-watchlist.js';

function displayStockTable(watchlist: WatchlistWithItemsResponse): void {
  if (watchlist.items.length === 0) {
    console.log(chalk.yellow('\nNo stocks in this watchlist.'));
    console.log(chalk.gray('\nAdd stocks with: watchlist add-stock <watchlist> <code>'));
    return;
  }

  console.log(chalk.bold('\n\nStocks:'));
  console.log(chalk.bold('-'.repeat(60)));

  const codeHeader = 'Code'.padEnd(8);
  const companyHeader = 'Company'.padEnd(30);
  const dateHeader = 'Added'.padEnd(15);

  console.log(chalk.bold.white(`${codeHeader}${companyHeader}${dateHeader}`));
  console.log(chalk.gray('-'.repeat(60)));

  for (const item of watchlist.items) {
    const code = chalk.cyan(item.code.padEnd(8));
    const company = item.companyName.substring(0, 28).padEnd(30);
    const addedDate = new Date(item.createdAt);
    const date = addedDate.toLocaleDateString('ja-JP').padEnd(15);

    console.log(`${code}${company}${date}`);

    if (item.memo) {
      console.log(chalk.gray(`        Memo: ${item.memo}`));
    }
  }

  console.log(chalk.gray('-'.repeat(60)));
  console.log(chalk.white(`Total Stocks: ${chalk.yellow(watchlist.items.length.toString())}`));
}

export const showCommand = define({
  name: 'show',
  description: 'Show watchlist details with all stocks',
  args: {
    nameOrId: {
      type: 'positional',
      description: 'Watchlist name or ID',
    },
    debug: {
      type: 'boolean',
      description: 'Enable debug logging',
    },
  },
  examples: `
# Show watchlist by name
${CLI_NAME} watchlist show "Tech Stocks"

# Show watchlist by ID
${CLI_NAME} watchlist show 1
  `.trim(),
  run: async (ctx) => {
    const { nameOrId, debug } = ctx.values;

    if (!nameOrId) {
      throw new CLIValidationError(`Watchlist name or ID is required\nUsage: ${CLI_NAME} watchlist show <name-or-id>`);
    }

    try {
      const apiClient = new ApiClient();

      if (debug) {
        console.log(chalk.gray('[DEBUG] Fetching watchlist from API'));
      }

      const watchlist = await resolveWatchlist(apiClient, nameOrId);

      // Display header
      console.log(chalk.bold.cyan(`\n${watchlist.name}`));
      console.log(chalk.bold('='.repeat(60)));

      if (watchlist.description) {
        console.log(chalk.white(watchlist.description));
        console.log();
      }

      console.log(chalk.gray(`ID: ${watchlist.id}`));
      const createdAt = new Date(watchlist.createdAt);
      console.log(chalk.gray(`Created: ${createdAt.toLocaleString('ja-JP')}`));

      displayStockTable(watchlist);

      console.log(chalk.bold('='.repeat(60)));
    } catch (error) {
      if (error instanceof CLIError) throw error;
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error(chalk.red(`✗ Failed to show watchlist: ${errorMessage}`));

      if (debug && error instanceof Error && error.stack) {
        console.error(chalk.gray(`\n[DEBUG] Stack trace:\n${error.stack}`));
      }

      throw new CLIError(errorMessage, 1, true, { cause: error });
    }
  },
});
