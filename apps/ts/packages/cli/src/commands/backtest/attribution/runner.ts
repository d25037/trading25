import { cli } from 'gunshi';

import { CLI_NAME, CLI_VERSION } from '../../../utils/constants.js';
import { attributionCommand } from './index.js';
import { cancelCommand } from './cancel.js';
import { resultsCommand } from './results.js';
import { runCommand } from './run.js';
import { statusCommand } from './status.js';

const subCommands = {
  run: runCommand,
  status: statusCommand,
  results: resultsCommand,
  cancel: cancelCommand,
};

export default async function attributionCommandRunner(args: string[]): Promise<void> {
  await cli(args, attributionCommand, {
    name: `${CLI_NAME} backtest attribution`,
    version: CLI_VERSION,
    description: 'Signal attribution operations (LOO + Shapley)',
    subCommands,
  });
}
