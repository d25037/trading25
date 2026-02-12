/**
 * Backtest CLI common error handler
 */

import { BacktestApiError } from '@trading25/clients-ts/backtest';
import chalk from 'chalk';

interface Logger {
  log: (msg: string) => void;
}

export function handleBacktestError(ctx: Logger, error: unknown): void {
  if (error instanceof BacktestApiError) {
    if (error.status === 404) {
      ctx.log(chalk.red(`Error: Not found - ${error.message}`));
    } else {
      ctx.log(chalk.red(`API Error (${error.status}): ${error.message}`));
    }
  } else if (error instanceof Error) {
    if (error.message.includes('ECONNREFUSED') || error.message.includes('fetch failed')) {
      ctx.log(chalk.red('Error: Cannot connect to bt server'));
      ctx.log(chalk.dim('Make sure bt server is running: uv run bt server --port 3002'));
    } else {
      ctx.log(chalk.red(`Error: ${error.message}`));
    }
  }
}
