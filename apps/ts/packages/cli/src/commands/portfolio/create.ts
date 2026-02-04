/**
 * Portfolio Create Command
 * Create a new portfolio via API
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import { ApiClient } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { CLIError, CLIValidationError } from '../../utils/error-handling.js';
/**
 * Create command definition
 */
export const createCommand = define({
  name: 'create',
  description: 'Create a new portfolio',
  args: {
    name: {
      type: 'positional',
      description: 'Portfolio name',
    },
    description: {
      type: 'string',
      short: 'd',
      description: 'Portfolio description',
    },
    debug: {
      type: 'boolean',
      description: 'Enable debug logging',
    },
  },
  examples: `
# Create a portfolio
${CLI_NAME} portfolio create "My Portfolio"

# Create with description
${CLI_NAME} portfolio create "Growth Stocks" -d "Long-term growth investments"
  `.trim(),
  run: async (ctx) => {
    const { name, description, debug } = ctx.values;

    if (!name) {
      throw new CLIValidationError(`Portfolio name is required\nUsage: ${CLI_NAME} portfolio create <name>`);
    }

    try {
      const apiClient = new ApiClient();

      if (debug) {
        console.log(chalk.gray('[DEBUG] Creating portfolio via API'));
      }

      // Create portfolio
      const portfolio = await apiClient.createPortfolio({
        name,
        description,
      });

      // Success message
      console.log(chalk.green(`✓ Created portfolio: ${chalk.bold(portfolio.name)}`));
      console.log(chalk.gray(`  ID: ${portfolio.id}`));
      if (portfolio.description) {
        console.log(chalk.gray(`  Description: ${portfolio.description}`));
      }
      const createdAt = new Date(portfolio.createdAt);
      console.log(chalk.gray(`  Created: ${createdAt.toLocaleString('ja-JP')}`));
    } catch (error) {
      if (error instanceof CLIError) throw error;
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error(chalk.red(`✗ Failed to create portfolio: ${errorMessage}`));

      if (debug && error instanceof Error && error.stack) {
        console.error(chalk.gray(`\n[DEBUG] Stack trace:\n${error.stack}`));
      }

      throw new CLIError(errorMessage, 1, true, { cause: error });
    }
  },
});
