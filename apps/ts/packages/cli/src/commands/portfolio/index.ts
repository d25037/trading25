/**
 * Portfolio Commands - Entry Point
 * Portfolio management operations with direct imports for full help display
 */

import { cli, define } from 'gunshi';
import { CLI_NAME, CLI_VERSION } from '../../utils/constants.js';
import { addStockCommand } from './add-stock.js';
import { createCommand } from './create.js';
import { deleteCommand } from './delete.js';
import { listCommand } from './list.js';
import { removeStockCommand } from './remove-stock.js';
import { showCommand } from './show.js';
import { updateStockCommand } from './update-stock.js';

// Portfolio group command definition (exported for help display)
export const portfolioCommand = define({
  name: 'portfolio',
  description: 'Portfolio management - track stock holdings across multiple portfolios',
  run: (ctx) => {
    ctx.log('Available commands: create, list, show, delete, add-stock, update-stock, remove-stock');
    ctx.log(`Use "${CLI_NAME} portfolio <command> --help" for more information`);
  },
});

// Direct imports for full args display in help
const subCommands = {
  create: createCommand,
  list: listCommand,
  show: showCommand,
  delete: deleteCommand,
  'add-stock': addStockCommand,
  'update-stock': updateStockCommand,
  'remove-stock': removeStockCommand,
};

// Export command runner for this group
export default async function portfolioCommandRunner(args: string[]): Promise<void> {
  await cli(args, portfolioCommand, {
    name: `${CLI_NAME} portfolio`,
    version: CLI_VERSION,
    description: 'Portfolio management - track stock holdings across multiple portfolios',
    subCommands,
  });
}
