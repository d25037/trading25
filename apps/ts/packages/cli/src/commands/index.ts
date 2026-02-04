/**
 * CLI Commands - Main Export
 * Lazy-loaded command registry for Gunshi
 */

import { define, lazy } from 'gunshi';

// Main command (shown when no subcommand is provided)
export const mainCommand = define({
  name: 'trading25',
  description: 'CLI for Trading25 - Japanese stock data analysis',
  run: (ctx) => {
    ctx.log('Use --help to see available commands');
  },
});

// Lazy-loaded analysis command (shared for alias)
const lazyAnalysis = lazy(async () => (await import('./analysis/index.js')).analysisCommand, {
  name: 'analysis',
  description: 'Financial analysis, screening, and ranking commands',
});

// Lazy-loaded subcommands (using exported command definitions for help display)
export const subCommands = {
  analysis: lazyAnalysis,
  analyze: lazyAnalysis, // Alias for analysis
  db: lazy(async () => (await import('./db/index.js')).dbCommand, {
    name: 'db',
    description: 'Database operations - sync, validate, refresh',
  }),
  dataset: lazy(async () => (await import('./dataset/index.js')).datasetCommand, {
    name: 'dataset',
    description: 'Dataset management - create, validate, info, sample, search',
  }),
  jquants: lazy(async () => (await import('./jquants/index.js')).jquantsCommand, {
    name: 'jquants',
    description: 'JQuants API operations - auth, fetch',
  }),
  portfolio: lazy(async () => (await import('./portfolio/index.js')).portfolioCommand, {
    name: 'portfolio',
    description: 'Portfolio management - track stock holdings',
  }),
};
