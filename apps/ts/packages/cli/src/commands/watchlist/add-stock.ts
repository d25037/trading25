import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';
import { ApiClient } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { CLIValidationError, handleCommandError } from '../../utils/error-handling.js';
import { resolveWatchlistId } from './resolve-watchlist.js';

export const addStockCommand = define({
  name: 'add-stock',
  description: 'Add a stock to a watchlist (company name fetched automatically)',
  args: {
    watchlist: {
      type: 'positional',
      description: 'Watchlist name or ID',
    },
    code: {
      type: 'positional',
      description: 'Stock code (e.g., 7203)',
    },
    memo: {
      type: 'string',
      short: 'm',
      description: 'Optional memo',
    },
    debug: {
      type: 'boolean',
      description: 'Enable debug logging',
    },
  },
  examples: `
# Add stock to watchlist
${CLI_NAME} watchlist add-stock "Tech Stocks" 7203

# Add with memo
${CLI_NAME} watchlist add-stock "Growth" 6758 -m "Watching for breakout"
  `.trim(),
  run: async (ctx) => {
    const { watchlist: watchlistNameOrId, code, memo, debug } = ctx.values;
    const spinner = ora('Initializing...').start();

    if (!watchlistNameOrId || !code) {
      spinner.fail(chalk.red('Watchlist name/ID and stock code are required'));
      throw new CLIValidationError(
        `Watchlist name/ID and stock code are required\nUsage: ${CLI_NAME} watchlist add-stock <watchlist> <code>`
      );
    }

    try {
      const apiClient = new ApiClient();

      spinner.text = 'Finding watchlist...';
      const resolvedId = await resolveWatchlistId(apiClient, watchlistNameOrId);

      spinner.text = 'Adding stock to watchlist...';
      const item = await apiClient.watchlist.addWatchlistItem(resolvedId, { code, memo });

      spinner.succeed(chalk.green(`✓ Added ${chalk.bold(item.companyName)} (${code}) to watchlist`));

      if (item.memo) {
        console.log(chalk.gray(`  Memo: ${item.memo}`));
      }
    } catch (error) {
      handleCommandError(error, spinner, {
        failMessage: 'Failed to add stock',
        debug,
      });
    }
  },
});
