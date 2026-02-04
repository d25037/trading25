/**
 * Dataset Commands - Entry Point
 * Dataset management operations with direct imports for full help display
 */

import { cli, define } from 'gunshi';
import { CLI_NAME, CLI_VERSION } from '../../utils/constants.js';
import { createCommand } from './create.js';
import { infoCommand } from './info.js';
import { sampleCommand } from './sample.js';
import { searchCommand } from './search.js';

// Dataset group command definition (exported for help display)
export const datasetCommand = define({
  name: 'dataset',
  description: 'Manage dataset snapshots (market data captured using a preset)',
  run: (ctx) => {
    ctx.log('Available commands: create, info, sample, search');
    ctx.log(`Use "${CLI_NAME} dataset <command> --help" for more information`);
  },
});

// Direct imports for full args display in help
const subCommands = {
  create: createCommand,
  info: infoCommand,
  sample: sampleCommand,
  search: searchCommand,
};

// Export command runner for this group
export default async function datasetCommandRunner(args: string[]): Promise<void> {
  await cli(args, datasetCommand, {
    name: `${CLI_NAME} dataset`,
    version: CLI_VERSION,
    description: 'Manage dataset snapshots (market data captured using a preset)',
    subCommands,
  });
}
