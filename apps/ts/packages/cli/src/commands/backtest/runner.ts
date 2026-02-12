/**
 * Backtest Commands - Runner
 *
 * バックテストコマンドグループのエントリーポイント
 */

import { cli } from 'gunshi';

import { CLI_NAME, CLI_VERSION } from '../../utils/constants.js';
import { attributionCommand } from './attribution/index.js';
import { backtestCommand } from './index.js';
import { cancelCommand } from './cancel.js';
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
  cancel: cancelCommand,
  attribution: attributionCommand,
};

// Export command runner for this group
export default async function backtestCommandRunner(args: string[]): Promise<void> {
  const subCommand = args[0];
  if (subCommand === 'attribution') {
    const attributionRunner = (await import('./attribution/runner.js')).default;
    await attributionRunner(args.slice(1));
    return;
  }

  await cli(args, backtestCommand, {
    name: `${CLI_NAME} backtest`,
    version: CLI_VERSION,
    description: 'Backtest operations - run strategies, check results',
    subCommands,
  });
}
