/**
 * Backtest List Command
 *
 * 戦略一覧表示コマンド
 */

import { BacktestClient, type StrategyMetadata } from '@trading25/clients-ts/backtest';
import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';

import { CLI_NAME } from '../../utils/constants.js';
import { CLIError } from '../../utils/error-handling.js';
import { handleBacktestError } from './error-handler.js';

export const listCommand = define({
  name: 'list',
  description: 'List available strategies',
  args: {
    category: {
      type: 'string',
      short: 'c',
      description: 'Filter by category (production, experimental, etc.)',
    },
    btUrl: {
      type: 'string',
      description: 'Backtest API server URL',
      default: process.env.BT_API_URL ?? 'http://localhost:3002',
    },
    format: {
      type: 'string',
      short: 'f',
      description: 'Output format: table, json',
      default: 'table',
    },
    debug: {
      type: 'boolean',
      description: 'Enable debug output',
    },
  },
  examples: `# List all strategies
${CLI_NAME} backtest list

# List strategies in a specific category
${CLI_NAME} backtest list --category production

# Output as JSON
${CLI_NAME} backtest list --format json`,
  run: async (ctx) => {
    const { category, btUrl, format, debug: _debug } = ctx.values;

    const client = new BacktestClient({ baseUrl: btUrl });
    const spinner = ora();

    try {
      spinner.start('Fetching strategies...');
      const response = await client.listStrategies();
      spinner.stop();

      // カテゴリでフィルタ
      let strategies = response.strategies;
      if (category) {
        strategies = strategies.filter((s) => s.category === category);
      }

      if (format === 'json') {
        ctx.log(JSON.stringify(strategies, null, 2));
        return;
      }

      // テーブル表示
      if (strategies.length === 0) {
        ctx.log(chalk.yellow('No strategies found'));
        return;
      }

      // カテゴリでグループ化
      const grouped = groupByCategory(strategies);

      ctx.log('');
      ctx.log(chalk.bold(`Available Strategies (${strategies.length} total)`));
      ctx.log('');

      for (const [cat, strats] of Object.entries(grouped)) {
        ctx.log(chalk.cyan.bold(`📁 ${cat}`));
        for (const s of strats) {
          const name = s.name.includes('/') ? s.name.split('/').pop() : s.name;
          const displayName = s.display_name ? chalk.dim(` - ${s.display_name}`) : '';
          ctx.log(`   ${chalk.white(name)}${displayName}`);
        }
        ctx.log('');
      }
    } catch (error) {
      spinner.fail('Failed to fetch strategies');
      handleBacktestError(ctx, error);
      throw new CLIError('Failed to fetch strategies', 1, true, { cause: error });
    }
  },
});

function groupByCategory(strategies: StrategyMetadata[]): Record<string, StrategyMetadata[]> {
  const grouped: Record<string, StrategyMetadata[]> = {};

  for (const s of strategies) {
    const cat = s.category || 'other';
    if (!grouped[cat]) {
      grouped[cat] = [];
    }
    grouped[cat].push(s);
  }

  // カテゴリ名でソート
  const sorted: Record<string, StrategyMetadata[]> = {};
  const order = ['production', 'experimental', 'reference', 'legacy', 'other'];

  for (const cat of order) {
    if (grouped[cat]) {
      sorted[cat] = grouped[cat].sort((a, b) => a.name.localeCompare(b.name));
    }
  }

  // その他のカテゴリ
  for (const cat of Object.keys(grouped).sort()) {
    if (!sorted[cat] && grouped[cat]) {
      sorted[cat] = grouped[cat].sort((a, b) => a.name.localeCompare(b.name));
    }
  }

  return sorted;
}
