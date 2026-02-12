/**
 * Backtest Command Group
 *
 * バックテストの実行・結果確認コマンド
 */

import { define } from 'gunshi';
import { attributionCommand } from './attribution/index.js';
import { cancelCommand } from './cancel.js';
import { listCommand } from './list.js';
import { resultsCommand } from './results.js';
import { runCommand } from './run.js';
import { statusCommand } from './status.js';
import { validateCommand } from './validate.js';

export const backtestCommand = define({
  name: 'backtest',
  description: 'Backtest operations - run strategies, check results',
  sub: {
    run: runCommand,
    list: listCommand,
    validate: validateCommand,
    results: resultsCommand,
    status: statusCommand,
    cancel: cancelCommand,
    attribution: attributionCommand,
  },
  run: (ctx) => {
    ctx.log('Use --help to see available backtest commands');
  },
});
