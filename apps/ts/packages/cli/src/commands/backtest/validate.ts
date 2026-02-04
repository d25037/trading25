/**
 * Backtest Validate Command
 *
 * 戦略設定検証コマンド
 */

import { BacktestClient } from '@trading25/shared/clients/backtest';
import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';

import { CLI_NAME } from '../../utils/constants.js';
import { CLIError, CLIValidationError } from '../../utils/error-handling.js';
import { handleBacktestError } from './error-handler.js';

export const validateCommand = define({
  name: 'validate',
  description: 'Validate a strategy configuration',
  args: {
    strategy: {
      type: 'positional',
      description: 'Strategy name to validate',
      required: true,
    },
    btUrl: {
      type: 'string',
      description: 'Backtest API server URL',
      default: process.env.BT_API_URL ?? 'http://localhost:3002',
    },
    debug: {
      type: 'boolean',
      description: 'Enable debug output',
    },
  },
  examples: `# Validate a strategy
${CLI_NAME} backtest validate range_break_v5

# Validate with specific path
${CLI_NAME} backtest validate production/range_break_v5`,
  run: async (ctx) => {
    const { strategy, btUrl, debug: _debug } = ctx.values;

    if (!strategy) {
      throw new CLIValidationError('Strategy name is required');
    }

    const client = new BacktestClient({ baseUrl: btUrl });
    const spinner = ora();

    try {
      spinner.start(`Validating strategy: ${chalk.cyan(strategy)}`);
      const result = await client.validateStrategy(strategy);
      spinner.stop();

      ctx.log('');

      if (result.valid) {
        ctx.log(chalk.green('✓ Strategy configuration is valid'));
      } else {
        ctx.log(chalk.red('✗ Strategy configuration has errors'));
      }

      if (result.errors.length > 0) {
        ctx.log('');
        ctx.log(chalk.red.bold('Errors:'));
        for (const error of result.errors) {
          ctx.log(chalk.red(`  • ${error}`));
        }
      }

      if (result.warnings.length > 0) {
        ctx.log('');
        ctx.log(chalk.yellow.bold('Warnings:'));
        for (const warning of result.warnings) {
          ctx.log(chalk.yellow(`  • ${warning}`));
        }
      }

      if (!result.valid) {
        throw new CLIError('Strategy configuration has errors', 1, true);
      }
    } catch (error) {
      if (error instanceof CLIError) {
        spinner.stop();
        throw error;
      }
      spinner.fail('Validation failed');
      handleBacktestError(ctx, error);
      throw new CLIError('Validation failed', 1, true, { cause: error });
    }
  },
});
