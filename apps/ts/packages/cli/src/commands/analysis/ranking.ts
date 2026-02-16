/**
 * Market Ranking Command
 * Display top stocks by trading value, price gains, and price losses
 * Uses API endpoint: GET /api/analytics/market/ranking
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';
import stringWidth from 'string-width';
import { ApiClient, type MarketRankingItem, type MarketRankingResponse } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { CLIError } from '../../utils/error-handling.js';

/**
 * Format number with commas
 */
function formatNumber(num: number): string {
  return num.toLocaleString('ja-JP');
}

/**
 * Format price in Japanese Yen
 */
function formatPrice(price: number): string {
  return `¥${formatNumber(price)}`;
}

/**
 * Format percentage
 */
function formatPercentage(num: number): string {
  const sign = num >= 0 ? '+' : '';
  return `${sign}${num.toFixed(2)}%`;
}

/**
 * Truncate string to fit within visual width
 */
function truncateString(str: string, maxWidth: number): string {
  if (!str || maxWidth <= 0) return '';

  const totalWidth = stringWidth(str);
  if (totalWidth <= maxWidth) return str;

  let result = '';
  let width = 0;

  for (const char of str) {
    const charWidth = stringWidth(char);
    if (width + charWidth > maxWidth) break;
    result += char;
    width += charWidth;
  }

  return result;
}

/**
 * Pad string to visual width
 */
function padEndVisual(str: string, targetWidth: number): string {
  if (!str) return ' '.repeat(Math.max(0, targetWidth));
  if (targetWidth <= 0) return str;

  const currentWidth = stringWidth(str);
  if (currentWidth >= targetWidth) return str;

  const paddingNeeded = targetWidth - currentWidth;
  return str + ' '.repeat(paddingNeeded);
}

/**
 * Display trading value ranking table
 */
function displayTradingValueRanking(items: MarketRankingItem[], title: string, lookbackDays: number): void {
  console.log(chalk.bold.cyan(`\n${title}`));
  console.log(chalk.gray('─'.repeat(120)));

  // Header
  const valueHeader = lookbackDays === 1 ? 'Trading Value' : `Avg Value (${lookbackDays}d)`;
  console.log(
    chalk.bold.white(padEndVisual('Rank', 6)) +
      chalk.bold.white(padEndVisual('Code', 8)) +
      chalk.bold.white(padEndVisual('Company', 25)) +
      chalk.bold.white(padEndVisual('Market', 10)) +
      chalk.bold.white(padEndVisual('Sector', 20)) +
      chalk.bold.white(padEndVisual('Price', 15)) +
      chalk.bold.white(padEndVisual('Volume', 15)) +
      chalk.bold.white(valueHeader)
  );

  console.log(chalk.gray('─'.repeat(120)));

  // Data rows
  for (const item of items) {
    const rank = chalk.yellow(padEndVisual(`${item.rank}`, 6));
    const code = chalk.cyan(padEndVisual(item.code, 8));
    const company = chalk.white(padEndVisual(truncateString(item.companyName, 25), 25));
    const market = chalk.blue(padEndVisual(item.marketCode, 10));
    const sector = chalk.magenta(padEndVisual(truncateString(item.sector33Name, 20), 20));
    const price = chalk.green(padEndVisual(formatPrice(item.currentPrice), 15));
    const volume = chalk.white(padEndVisual(formatNumber(item.volume), 15));
    const tradingValue = chalk.green(formatPrice(item.tradingValueAverage || item.tradingValue || 0));

    console.log(rank + code + company + market + sector + price + volume + tradingValue);
  }

  console.log(chalk.gray('─'.repeat(120)));
}

/**
 * Display price change ranking table
 */
function displayPriceChangeRanking(items: MarketRankingItem[], title: string, lookbackDays: number): void {
  console.log(chalk.bold.cyan(`\n${title}`));
  console.log(chalk.gray('─'.repeat(120)));

  // Header
  const priceLabel = lookbackDays === 1 ? 'Price' : 'Current';
  const baseLabel = lookbackDays === 1 ? '' : `Base(${lookbackDays}d)`;
  const baseWidth = lookbackDays === 1 ? 0 : 15;

  let header =
    chalk.bold.white(padEndVisual('Rank', 6)) +
    chalk.bold.white(padEndVisual('Code', 8)) +
    chalk.bold.white(padEndVisual('Company', 25)) +
    chalk.bold.white(padEndVisual('Market', 10)) +
    chalk.bold.white(padEndVisual('Sector', 20)) +
    chalk.bold.white(padEndVisual(priceLabel, 15));

  if (lookbackDays > 1) {
    header += chalk.bold.white(padEndVisual(baseLabel, baseWidth));
  }

  header += chalk.bold.white(padEndVisual('Change', 15)) + chalk.bold.white('Change %');

  console.log(header);
  console.log(chalk.gray('─'.repeat(120)));

  // Data rows
  for (const item of items) {
    const rank = chalk.yellow(padEndVisual(`${item.rank}`, 6));
    const code = chalk.cyan(padEndVisual(item.code, 8));
    const company = chalk.white(padEndVisual(truncateString(item.companyName, 25), 25));
    const market = chalk.blue(padEndVisual(item.marketCode, 10));
    const sector = chalk.magenta(padEndVisual(truncateString(item.sector33Name, 20), 20));
    const price = chalk.white(padEndVisual(formatPrice(item.currentPrice), 15));

    let row = rank + code + company + market + sector + price;

    if (lookbackDays > 1 && item.basePrice !== undefined) {
      const basePrice = chalk.gray(padEndVisual(formatPrice(item.basePrice), baseWidth));
      row += basePrice;
    }

    const changeAmount = item.changeAmount || 0;
    const changePercentage = item.changePercentage || 0;

    const changeColor = changeAmount >= 0 ? chalk.green : chalk.red;
    const change = changeColor(padEndVisual(formatPrice(changeAmount), 15));
    const changePct = changeColor(formatPercentage(changePercentage));

    row += change + changePct;
    console.log(row);
  }

  console.log(chalk.gray('─'.repeat(120)));
}

/**
 * Display rankings as JSON
 */
function displayAsJSON(response: MarketRankingResponse): void {
  console.log(JSON.stringify(response, null, 2));
}

/**
 * Display rankings as CSV
 */
function displayAsCSV(rankings: MarketRankingResponse['rankings']): void {
  // Trading Value CSV
  console.log('\n# Trading Value Ranking');
  console.log('Rank,Code,Company,Market,Sector,Price,Volume,TradingValue');
  for (const item of rankings.tradingValue) {
    console.log(
      `${item.rank},"${item.code}","${item.companyName.replace(/"/g, '""')}","${item.marketCode}","${item.sector33Name}",${item.currentPrice},${item.volume},${item.tradingValue || item.tradingValueAverage || 0}`
    );
  }

  // Gainers CSV
  console.log('\n# Top Gainers');
  console.log('Rank,Code,Company,Market,Sector,Price,PreviousPrice,Change,ChangePercentage');
  for (const item of rankings.gainers) {
    console.log(
      `${item.rank},"${item.code}","${item.companyName.replace(/"/g, '""')}","${item.marketCode}","${item.sector33Name}",${item.currentPrice},${item.previousPrice || item.basePrice || 0},${item.changeAmount || 0},${item.changePercentage || 0}`
    );
  }

  // Losers CSV
  console.log('\n# Top Losers');
  console.log('Rank,Code,Company,Market,Sector,Price,PreviousPrice,Change,ChangePercentage');
  for (const item of rankings.losers) {
    console.log(
      `${item.rank},"${item.code}","${item.companyName.replace(/"/g, '""')}","${item.marketCode}","${item.sector33Name}",${item.currentPrice},${item.previousPrice || item.basePrice || 0},${item.changeAmount || 0},${item.changePercentage || 0}`
    );
  }
}

/**
 * Parse and validate ranking options
 */
interface ParsedOptions {
  limit: number;
  lookbackDays: number;
  markets: string;
  format: 'table' | 'json' | 'csv';
}

interface RankingOptions {
  date?: string;
  limit?: string;
  markets?: string;
  format?: string;
  lookbackDays?: string;
  debug?: boolean;
}

function parseRankingOptions(options: RankingOptions): ParsedOptions {
  const limit = options.limit ? Number.parseInt(options.limit, 10) : 20;
  if (Number.isNaN(limit) || limit < 1 || limit > 100) {
    throw new Error('Limit must be a number between 1 and 100');
  }

  const lookbackDays = options.lookbackDays ? Number.parseInt(options.lookbackDays, 10) : 1;
  if (Number.isNaN(lookbackDays) || lookbackDays < 1 || lookbackDays > 100) {
    throw new Error('Lookback days must be a number between 1 and 100');
  }

  const markets = options.markets || 'prime';

  // Validate market codes
  const marketCodes = markets.split(',').map((m) => m.trim());
  const validMarkets = ['prime', 'standard'];
  for (const market of marketCodes) {
    if (!validMarkets.includes(market)) {
      throw new Error(`Invalid market: ${market}. Valid options: prime, standard`);
    }
  }

  return {
    limit,
    lookbackDays,
    markets,
    format: (options.format || 'table') as 'table' | 'json' | 'csv',
  };
}

/**
 * Display rankings in table format
 */
function displayTableFormat(response: MarketRankingResponse, limit: number): void {
  const { date, markets, lookbackDays, rankings } = response;
  const marketLabel = markets.map((m) => m.charAt(0).toUpperCase() + m.slice(1)).join(', ');
  const periodLabel = lookbackDays === 1 ? '(vs Previous Day)' : `(vs ${lookbackDays} Trading Days Ago)`;

  console.log(chalk.bold.white(`\n${'='.repeat(120)}`));
  console.log(chalk.bold.cyan(`📊 Market Ranking - ${date} ${periodLabel} (${marketLabel} Market)`));
  console.log(chalk.bold.white('='.repeat(120)));

  if (rankings.tradingValue.length > 0) {
    const tradingValueTitle =
      lookbackDays === 1
        ? `💰 Top ${limit} by Trading Value`
        : `💰 Top ${limit} by Avg Trading Value (${lookbackDays} days)`;
    displayTradingValueRanking(rankings.tradingValue, tradingValueTitle, lookbackDays);
  } else {
    console.log(chalk.yellow('\n💰 No trading value data found'));
  }

  if (rankings.gainers.length > 0) {
    const gainersTitle =
      lookbackDays === 1
        ? `📈 Top ${limit} Gainers (by Price Change)`
        : `📈 Top ${limit} Gainers (${lookbackDays} days)`;
    displayPriceChangeRanking(rankings.gainers, gainersTitle, lookbackDays);
  } else {
    console.log(chalk.yellow(`\n📈 No gainers data found (${lookbackDays} trading days ago data may be missing)`));
  }

  if (rankings.losers.length > 0) {
    const losersTitle =
      lookbackDays === 1 ? `📉 Top ${limit} Losers (by Price Change)` : `📉 Top ${limit} Losers (${lookbackDays} days)`;
    displayPriceChangeRanking(rankings.losers, losersTitle, lookbackDays);
  } else {
    console.log(chalk.yellow(`\n📉 No losers data found (${lookbackDays} trading days ago data may be missing)`));
  }

  console.log(chalk.bold.white(`\n${'='.repeat(120)}\n`));
}

/**
 * Execute market ranking command via API
 */
async function executeMarketRanking(options: RankingOptions): Promise<void> {
  const parsed = parseRankingOptions(options);

  if (options.debug) {
    console.log(chalk.gray(`Market filter: ${parsed.markets}`));
    console.log(chalk.gray(`Limit: ${parsed.limit}`));
    console.log(chalk.gray(`Lookback days: ${parsed.lookbackDays}`));
  }

  const apiClient = new ApiClient();
  const spinner = ora('Loading market rankings from API...').start();

  try {
    const response = await apiClient.analytics.getMarketRanking({
      date: options.date,
      limit: parsed.limit,
      markets: parsed.markets,
      lookbackDays: parsed.lookbackDays,
    });

    spinner.succeed(chalk.green('Rankings loaded successfully'));

    if (options.debug) {
      console.log(chalk.gray(`Using date: ${response.date}`));
    }

    if (parsed.format === 'json') {
      displayAsJSON(response);
    } else if (parsed.format === 'csv') {
      displayAsCSV(response.rankings);
    } else {
      displayTableFormat(response, parsed.limit);
    }
  } catch (error) {
    spinner.fail(chalk.red('Failed to load rankings'));
    throw error;
  }
}

/**
 * Handle ranking errors
 */
function handleRankingError(error: unknown): void {
  console.error(chalk.red('\n❌ Market ranking failed:'));

  if (error instanceof Error) {
    console.error(chalk.red(`   Error: ${error.message}`));

    if (process.env.DEBUG) {
      console.error(chalk.gray('   Stack trace:'));
      console.error(chalk.gray(error.stack));
    }
  } else {
    const errorString = String(error);
    console.error(chalk.red(`   ${errorString}`));
  }

  console.error(chalk.gray('\n💡 Troubleshooting tips:'));
  console.error(chalk.gray('   • Ensure API server is running: uv run bt server --port 3002'));
  console.error(chalk.gray(`   • Ensure market.db exists: ${CLI_NAME} db sync`));
  console.error(chalk.gray('   • Try with --debug flag for more information'));
}

/**
 * Ranking command definition
 */
export const rankingCommand = define({
  name: 'ranking',
  description: 'Display top stocks by trading value, price gains, and price losses',
  args: {
    date: {
      type: 'string',
      description: 'Target date (YYYY-MM-DD, default: latest trading date)',
    },
    limit: {
      type: 'string',
      description: 'Number of stocks to display per ranking (default: 20, max: 100)',
      default: '20',
    },
    markets: {
      type: 'string',
      description: 'Market filter (prime|standard|prime,standard)',
      default: 'prime',
    },
    lookbackDays: {
      type: 'string',
      description: 'Number of trading days to look back for comparison (default: 1)',
      default: '1',
    },
    format: {
      type: 'string',
      description: 'Output format (table|json|csv)',
      default: 'table',
    },
    debug: {
      type: 'boolean',
      description: 'Enable debug output',
    },
  },
  examples: `
# Display latest market rankings (Prime market, top 20)
${CLI_NAME} analysis ranking

# Display rankings for a specific date
${CLI_NAME} analysis ranking --date 2025-10-01

# Display top 10 stocks
${CLI_NAME} analysis ranking --limit 10

# Compare with 5 trading days ago
${CLI_NAME} analysis ranking --lookback-days 5

# Include both Prime and Standard markets
${CLI_NAME} analysis ranking --markets prime,standard

# Output as JSON
${CLI_NAME} analysis ranking --format json

# Output as CSV
${CLI_NAME} analysis ranking --format csv
  `.trim(),
  run: async (ctx) => {
    const { date, limit, markets, lookbackDays, format, debug } = ctx.values;

    try {
      await executeMarketRanking({
        date,
        limit,
        markets,
        lookbackDays,
        format,
        debug,
      });
    } catch (error: unknown) {
      handleRankingError(error);
      throw new CLIError('Ranking analysis failed', 1, true, { cause: error });
    }
  },
});
