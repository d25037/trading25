/**
 * Backtest Commands - Runner
 *
 * バックテストコマンドグループのエントリーポイント
 */

import { cli } from 'gunshi';

import { CLI_NAME, CLI_VERSION } from '../../utils/constants.js';
import { backtestCommand } from './index.js';
import { listCommand } from './list.js';
import { resultsCommand } from './results.js';
import { runCommand } from './run.js';
import { statusCommand } from './status.js';
import { validateCommand } from './validate.js';

// Direct imports for full args display in help
const subCommands = {
  run: runCommand,
  list: listCommand,
  validate: validateCommand,
  results: resultsCommand,
  status: statusCommand,
};

// Export command runner for this group
export default async function backtestCommandRunner(args: string[]): Promise<void> {
  await cli(args, backtestCommand, {
    name: `${CLI_NAME} backtest`,
    version: CLI_VERSION,
    description: 'Backtest operations - run strategies, check results',
    subCommands,
  });
}
