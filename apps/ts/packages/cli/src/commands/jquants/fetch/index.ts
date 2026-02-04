/**
 * JQuants Fetch Commands - Entry Point
 * Data fetching commands for JQuants API with direct imports for full help display
 */

import { cli, define } from 'gunshi';
import { CLI_NAME, CLI_VERSION } from '../../../utils/constants.js';
import { dailyQuotesCommand } from './daily-quotes.js';
import { indicesCommand } from './indices.js';
import { listedInfoCommand } from './listed-info.js';
import { marginCommand } from './margin-interest.js';
import { testDataCommand } from './test-data.js';
import { topixCommand } from './topix.js';

// Fetch group command definition
const fetchCommand = define({
  name: 'fetch',
  description: 'Fetch data from JQuants API',
  run: (ctx) => {
    ctx.log('Available commands: daily-quotes, listed-info, margin, indices, topix, test-data');
    ctx.log(`Use "${CLI_NAME} jquants fetch <command> --help" for more information`);
  },
});

// Direct imports for full args display in help
const subCommands = {
  'daily-quotes': dailyQuotesCommand,
  'listed-info': listedInfoCommand,
  margin: marginCommand,
  indices: indicesCommand,
  topix: topixCommand,
  'test-data': testDataCommand,
};

// Export command runner for this group
export default async function fetchCommandRunner(args: string[]): Promise<void> {
  await cli(args, fetchCommand, {
    name: `${CLI_NAME} jquants fetch`,
    version: CLI_VERSION,
    description: 'Fetch data from JQuants API',
    subCommands,
  });
}
