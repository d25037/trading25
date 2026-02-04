/**
 * Watchlist Commands - Entry Point
 * Lightweight stock monitoring list operations
 */

import { cli, define } from 'gunshi';
import { CLI_NAME, CLI_VERSION } from '../../utils/constants.js';
import { addStockCommand } from './add-stock.js';
import { createCommand } from './create.js';
import { deleteCommand } from './delete.js';
import { listCommand } from './list.js';
import { removeStockCommand } from './remove-stock.js';
import { showCommand } from './show.js';

export const watchlistCommand = define({
  name: 'watchlist',
  description: 'Watchlist management - monitor stocks without portfolio details',
  run: (ctx) => {
    ctx.log('Available commands: create, list, show, delete, add-stock, remove-stock');
    ctx.log(`Use "${CLI_NAME} watchlist <command> --help" for more information`);
  },
});

const subCommands = {
  create: createCommand,
  list: listCommand,
  show: showCommand,
  delete: deleteCommand,
  'add-stock': addStockCommand,
  'remove-stock': removeStockCommand,
};

export default async function watchlistCommandRunner(args: string[]): Promise<void> {
  await cli(args, watchlistCommand, {
    name: `${CLI_NAME} watchlist`,
    version: CLI_VERSION,
    description: 'Watchlist management - monitor stocks without portfolio details',
    subCommands,
  });
}
