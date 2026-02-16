#!/usr/bin/env node

/**
 * Trading25 CLI - Entry Point
 * Built with Gunshi for modern CLI experience
 */

// Note: Bun automatically loads .env files from project root
import { cli, define } from 'gunshi';
import { CLI_NAME, CLI_VERSION } from './utils/constants.js';
import { CLIError } from './utils/error-handling.js';

// Main command definition
const mainCommand = define({
  name: 'trading25',
  description: 'CLI for Trading25 - Japanese stock data analysis',
  run: (ctx) => {
    ctx.log('Use --help to see available commands');
  },
});

// Get command line arguments
const args = process.argv.slice(2);
const subCommand = args[0];

// Command dispatch
async function main(): Promise<void> {
  // Handle sub-commands with their own cli() calls for proper nested command support
  if (subCommand === 'db') {
    const dbRunner = (await import('./commands/db/index.js')).default;
    await dbRunner(args.slice(1));
    return;
  }

  if (subCommand === 'dataset') {
    const datasetRunner = (await import('./commands/dataset/index.js')).default;
    await datasetRunner(args.slice(1));
    return;
  }

  if (subCommand === 'analysis' || subCommand === 'analyze') {
    const analysisRunner = (await import('./commands/analysis/index.js')).default;
    await analysisRunner(args.slice(1));
    return;
  }

  if (subCommand === 'jquants') {
    const jquantsRunner = (await import('./commands/jquants/index.js')).default;
    await jquantsRunner(args.slice(1));
    return;
  }

  if (subCommand === 'backtest' || subCommand === 'bt') {
    const backtestRunner = (await import('./commands/backtest/runner.js')).default;
    await backtestRunner(args.slice(1));
    return;
  }

  // For top-level commands (--help, --version, unknown commands)
  // Use lazy imports for descriptions in help output
  const { lazy } = await import('gunshi');

  const subCommands = {
    db: lazy(async () => mainCommand, {
      name: 'db',
      description: 'Database operations - sync, validate, refresh',
    }),
    dataset: lazy(async () => mainCommand, {
      name: 'dataset',
      description: 'Dataset management - create, info, sample, search',
    }),
    analysis: lazy(async () => mainCommand, {
      name: 'analysis',
      description: 'Financial analysis, screening, and ranking commands',
    }),
    analyze: lazy(async () => mainCommand, {
      name: 'analyze',
      description: 'Alias for analysis commands',
    }),
    jquants: lazy(async () => mainCommand, {
      name: 'jquants',
      description: 'JQuants API operations - auth, fetch',
    }),
    backtest: lazy(async () => mainCommand, {
      name: 'backtest',
      description: 'Backtest operations - run strategies, check results',
    }),
    bt: lazy(async () => mainCommand, {
      name: 'bt',
      description: 'Alias for backtest commands',
    }),
  };

  await cli(args, mainCommand, {
    name: CLI_NAME,
    version: CLI_VERSION,
    description: 'CLI for Trading25 - Japanese stock data analysis',
    subCommands,
  });
}

main().catch((error) => {
  if (error instanceof CLIError) {
    if (!error.silent) {
      console.error(error.message);
    }
    process.exitCode = error.exitCode;
    return;
  }
  console.error('CLI Error:', error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
