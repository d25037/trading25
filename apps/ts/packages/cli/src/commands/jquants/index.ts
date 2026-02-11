/**
 * JQuants Commands - Entry Point
 * JQuants API commands for fetching Japanese stock market data with lazy-loaded subcommands
 */

import chalk from 'chalk';
import { cli, define, lazy } from 'gunshi';
import { CLI_NAME, CLI_VERSION } from '../../utils/constants.js';
import { CLIError } from '../../utils/error-handling.js';

// JQuants group command definition (exported for help display)
export const jquantsCommand = define({
  name: 'jquants',
  description: 'JQuants API commands for fetching Japanese stock market data',
  run: (ctx) => {
    ctx.log('Available commands: auth, fetch');
    ctx.log(`Use "${CLI_NAME} jquants <command> --help" for more information`);
  },
});

// Get command line arguments for nested dispatch
async function jquantsCommandRunner(args: string[]): Promise<void> {
  const subCommand = args[0];

  // Handle nested auth commands
  if (subCommand === 'auth') {
    const authRunner = (await import('./auth/index.js')).default;
    await authRunner(args.slice(1));
    return;
  }

  // Handle nested fetch commands
  if (subCommand === 'fetch') {
    const fetchRunner = (await import('./fetch/index.js')).default;
    await fetchRunner(args.slice(1));
    return;
  }

  // For top-level jquants commands (--help, --version, unknown)
  const subCommands = {
    auth: lazy(async () => jquantsCommand, {
      name: 'auth',
      description: 'Authentication commands for JQuants API',
    }),
    fetch: lazy(async () => jquantsCommand, {
      name: 'fetch',
      description: 'Fetch data from JQuants API',
    }),
  };

  await cli(args, jquantsCommand, {
    name: `${CLI_NAME} jquants`,
    version: CLI_VERSION,
    description: 'JQuants API commands for fetching Japanese stock market data',
    subCommands,
  });
}

// Export command runner for this group
export default jquantsCommandRunner;

export function handleApiError(error: unknown, message: string): never {
  console.error(chalk.red(`\n❌ ${message}`));
  if (error instanceof Error) {
    console.error(chalk.gray(error.message));
    if (error.message.includes('authentication') || error.message.includes('token')) {
      console.log(
        chalk.yellow(`\n💡 Tip: Run "${CLI_NAME} jquants auth refresh-tokens" to update your authentication`)
      );
    }
  }
  throw new CLIError(message, 1, true, { cause: error });
}
