/**
 * Output Formatter
 * Format screening results for different output formats
 */

import type { ScreeningResultItem } from '@trading25/shared/types/api-response-types';
import chalk from 'chalk';
import stringWidth from 'string-width';

interface FormatterOptions {
  format: 'table' | 'json' | 'csv';
  verbose: boolean;
  debug: boolean;
}

function truncateString(str: string, maxWidth: number): string {
  if (!str || maxWidth <= 0) return '';
  if (stringWidth(str) <= maxWidth) return str;

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

function padEndVisual(str: string, targetWidth: number): string {
  const currentWidth = stringWidth(str || '');
  if (currentWidth >= targetWidth) return str;
  return str + ' '.repeat(targetWidth - currentWidth);
}

function formatDate(date: string): string {
  return date.split('T')[0] || '';
}

function formatScore(score: number | null): string {
  if (score === null || score === undefined) return 'N/A';
  return score.toFixed(3);
}

function printTableHeader(): void {
  const header =
    '\n' +
    chalk.bold.white('Code'.padEnd(8)) +
    chalk.bold.white('Company'.padEnd(20)) +
    chalk.bold.white('Sector33'.padEnd(15)) +
    chalk.bold.white('Best Strategy'.padEnd(20)) +
    chalk.bold.white('Score'.padEnd(10)) +
    chalk.bold.white('Matches'.padEnd(8)) +
    chalk.bold.white('Date'.padEnd(12));

  console.log(header);
  console.log(chalk.gray('─'.repeat(95)));
}

function formatTableRow(result: ScreeningResultItem): string {
  const code = chalk.cyan(padEndVisual(result.stockCode, 8));
  const company = chalk.white(padEndVisual(truncateString(result.companyName, 20), 20));
  const sector33 = chalk.magenta(padEndVisual(truncateString(result.sector33Name || 'N/A', 15), 15));
  const strategy = chalk.yellow(padEndVisual(truncateString(result.bestStrategyName, 20), 20));
  const scoreRaw = formatScore(result.bestStrategyScore);
  const score =
    result.bestStrategyScore === null
      ? chalk.gray(padEndVisual(scoreRaw, 10))
      : chalk.green(padEndVisual(scoreRaw, 10));
  const matches = chalk.white(padEndVisual(String(result.matchStrategyCount), 8));
  const date = chalk.green(padEndVisual(formatDate(result.matchedDate), 12));

  return code + company + sector33 + strategy + score + matches + date;
}

function printTableSummary(results: ScreeningResultItem[]): void {
  const topStrategies = Object.entries(
    results.reduce<Record<string, number>>((acc, row) => {
      acc[row.bestStrategyName] = (acc[row.bestStrategyName] || 0) + 1;
      return acc;
    }, {})
  )
    .sort((a, b) => b[1] - a[1])
    .slice(0, 3)
    .map(([name, count]) => `${name}(${count})`)
    .join(', ');

  console.log(chalk.gray('─'.repeat(95)));
  if (topStrategies) {
    console.log(chalk.white(`Total: ${results.length} stocks`) + chalk.gray(` (Top: ${topStrategies})`));
  } else {
    console.log(chalk.white(`Total: ${results.length} stocks`));
  }
}

function formatAsTable(results: ScreeningResultItem[], options: FormatterOptions): void {
  if (results.length === 0) {
    console.log(chalk.yellow('No results to display'));
    return;
  }

  printTableHeader();

  for (const result of results) {
    console.log(formatTableRow(result));

    if (options.verbose) {
      const matched = result.matchedStrategies
        .map((strategy) => `${strategy.strategyName}:${formatScore(strategy.strategyScore)}`)
        .join(', ');
      console.log(chalk.gray(`        matched: ${matched}`));
    }
  }

  printTableSummary(results);
}

function formatAsJSON(results: ScreeningResultItem[]): void {
  const output = {
    summary: {
      total: results.length,
      byBestStrategy: results.reduce<Record<string, number>>((acc, result) => {
        acc[result.bestStrategyName] = (acc[result.bestStrategyName] || 0) + 1;
        return acc;
      }, {}),
      noScoreCount: results.filter((result) => result.bestStrategyScore === null).length,
    },
    results,
  };

  console.log(JSON.stringify(output, null, 2));
}

function getCSVHeaders(verbose: boolean): string[] {
  const headers = [
    'StockCode',
    'CompanyName',
    'ScaleCategory',
    'Sector33Name',
    'MatchedDate',
    'BestStrategyName',
    'BestStrategyScore',
    'MatchStrategyCount',
  ];

  if (verbose) {
    headers.push('MatchedStrategies');
  }

  return headers;
}

function formatCSVRow(result: ScreeningResultItem, verbose: boolean): string {
  const row = [
    `"${result.stockCode}"`,
    `"${result.companyName.replace(/"/g, '""')}"`,
    `"${(result.scaleCategory || '').replace(/"/g, '""')}"`,
    `"${(result.sector33Name || '').replace(/"/g, '""')}"`,
    `"${result.matchedDate}"`,
    `"${result.bestStrategyName.replace(/"/g, '""')}"`,
    result.bestStrategyScore === null ? '' : result.bestStrategyScore.toString(),
    result.matchStrategyCount.toString(),
  ];

  if (verbose) {
    const matched = result.matchedStrategies
      .map((strategy) => `${strategy.strategyName}:${formatScore(strategy.strategyScore)}`)
      .join('|');
    row.push(`"${matched.replace(/"/g, '""')}"`);
  }

  return row.join(',');
}

function formatAsCSV(results: ScreeningResultItem[], options: FormatterOptions): void {
  const headers = getCSVHeaders(options.verbose);
  console.log(headers.join(','));

  for (const result of results) {
    console.log(formatCSVRow(result, options.verbose));
  }
}

export async function formatResults(results: ScreeningResultItem[], options: FormatterOptions): Promise<void> {
  switch (options.format) {
    case 'table':
      formatAsTable(results, options);
      break;
    case 'json':
      formatAsJSON(results);
      break;
    case 'csv':
      formatAsCSV(results, options);
      break;
    default:
      throw new Error(`Unsupported format: ${options.format}`);
  }
}
