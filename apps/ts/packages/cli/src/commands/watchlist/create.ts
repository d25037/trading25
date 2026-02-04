/**
 * Watchlist Create Command
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import { ApiClient } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { CLIError, CLIValidationError } from '../../utils/error-handling.js';

export const createCommand = define({
  name: 'create',
  description: 'Create a new watchlist',
  args: {
    name: {
      type: 'positional',
      description: 'Watchlist name',
    },
    description: {
      type: 'string',
      short: 'd',
      description: 'Watchlist description',
    },
    debug: {
      type: 'boolean',
      description: 'Enable debug logging',
    },
  },
  examples: `
# Create a watchlist
${CLI_NAME} watchlist create "Tech Stocks"

# Create with description
${CLI_NAME} watchlist create "Growth" -d "Growth stocks to monitor"
  `.trim(),
  run: async (ctx) => {
    const { name, description, debug } = ctx.values;

    if (!name) {
      throw new CLIValidationError(`Watchlist name is required\nUsage: ${CLI_NAME} watchlist create <name>`);
    }

    try {
      const apiClient = new ApiClient();

      if (debug) {
        console.log(chalk.gray('[DEBUG] Creating watchlist via API'));
      }

      const watchlist = await apiClient.createWatchlist({ name, description });

      console.log(chalk.green(`✓ Created watchlist: ${chalk.bold(watchlist.name)}`));
      console.log(chalk.gray(`  ID: ${watchlist.id}`));
      if (watchlist.description) {
        console.log(chalk.gray(`  Description: ${watchlist.description}`));
      }
      const createdAt = new Date(watchlist.createdAt);
      console.log(chalk.gray(`  Created: ${createdAt.toLocaleString('ja-JP')}`));
    } catch (error) {
      if (error instanceof CLIError) throw error;
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error(chalk.red(`✗ Failed to create watchlist: ${errorMessage}`));

      if (debug && error instanceof Error && error.stack) {
        console.error(chalk.gray(`\n[DEBUG] Stack trace:\n${error.stack}`));
      }

      throw new CLIError(errorMessage, 1, true, { cause: error });
    }
  },
});
