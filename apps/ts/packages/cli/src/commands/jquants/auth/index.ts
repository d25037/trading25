/**
 * JQuants Auth Commands - Entry Point
 * Authentication commands for JQuants API with direct imports for full help display
 */

import { cli, define } from 'gunshi';
import { CLI_NAME, CLI_VERSION } from '../../../utils/constants.js';
import { clearCommand } from './clear.js';
import { refreshTokensCommand } from './refresh-tokens.js';
import { statusCommand } from './status.js';

// Auth group command definition
const authCommand = define({
  name: 'auth',
  description: 'Authentication commands for JQuants API',
  run: (ctx) => {
    ctx.log('Available commands: refresh-tokens, status, clear');
    ctx.log(`Use "${CLI_NAME} jquants auth <command> --help" for more information`);
  },
});

// Direct imports for full args display in help
const subCommands = {
  'refresh-tokens': refreshTokensCommand,
  status: statusCommand,
  clear: clearCommand,
};

// Export command runner for this group
export default async function authCommandRunner(args: string[]): Promise<void> {
  await cli(args, authCommand, {
    name: `${CLI_NAME} jquants auth`,
    version: CLI_VERSION,
    description: 'Authentication commands for JQuants API',
    subCommands,
  });
}
