/**
 * Database Commands - Entry Point
 * Database synchronization and management operations with direct imports for full help display
 */

import { cli, define } from 'gunshi';
import { CLI_NAME, CLI_VERSION } from '../../utils/constants.js';
import { refreshCommand } from './refresh.js';
import { statsCommand } from './stats.js';
import { syncCommand } from './sync.js';
import { validateCommand } from './validate.js';

// DB group command definition (exported for help display)
export const dbCommand = define({
  name: 'db',
  description: 'Database synchronization and management',
  run: (ctx) => {
    ctx.log('Available commands: sync, validate, refresh, stats');
    ctx.log(`Use "${CLI_NAME} db <command> --help" for more information`);
  },
});

// Direct imports for full args display in help
const subCommands = {
  sync: syncCommand,
  validate: validateCommand,
  refresh: refreshCommand,
  stats: statsCommand,
};

// Export command runner for this group
export default async function dbCommandRunner(args: string[]): Promise<void> {
  await cli(args, dbCommand, {
    name: `${CLI_NAME} db`,
    version: CLI_VERSION,
    description: 'Database synchronization and management',
    subCommands,
  });
}
