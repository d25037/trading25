/**
 * Output Formatter
 * Format screening results for different output formats
 */

import type {
  FutureReturns,
  RangeBreakDetails,
  ScreeningResultItem,
} from '@trading25/shared/types/api-response-types';
import chalk from 'chalk';
import stringWidth from 'string-width';

interface FormatterOptions {
  format: 'table' | 'json' | 'csv';
  verbose: boolean;
  debug: boolean;
}

/**
 * Truncate string to fit within visual width
 * Takes into account full-width characters (Japanese, Chinese, etc.)
 */
function truncateString(str: string, maxWidth: number): string {
  // Handle edge cases
  if (!str || maxWidth <= 0) {
    return '';
  }

  // Quick path: if the string is already short enough
  const totalWidth = stringWidth(str);
  if (totalWidth <= maxWidth) {
    return str;
  }

  // Truncate character by character
  let result = '';
  let width = 0;

  for (const char of str) {
    const charWidth = stringWidth(char);
    if (width + charWidth > maxWidth) {
      break;
    }
    result += char;
    width += charWidth;
  }

  return result;
}

/**
 * Pad string to visual width
 * Takes into account full-width characters (Japanese, Chinese, etc.)
 */
function padEndVisual(str: string, targetWidth: number): string {
  // Handle edge cases
  if (!str) {
    return ' '.repeat(Math.max(0, targetWidth));
  }
  if (targetWidth <= 0) {
    return str;
  }

  const currentWidth = stringWidth(str);
  if (currentWidth >= targetWidth) {
    return str;
  }

  const paddingNeeded = targetWidth - currentWidth;
  return str + ' '.repeat(paddingNeeded);
}

/**
 * Format date for display
 */
function formatDate(date: string): string {
  return date.split('T')[0] || '';
}

/**
 * Format number with specified decimal places
 */
function formatNumber(num: number, decimals: number = 2): string {
  return num.toFixed(decimals);
}

/**
 * Format percentage
 */
function formatPercentage(num: number): string {
  return `${formatNumber(num, 2)}%`;
}

/**
 * Format percentage with sign and color
 */
function formatPercentageWithColor(num: number): string {
  const sign = num >= 0 ? '+' : '';
  const formatted = `${sign}${formatNumber(num, 1)}%`;
  if (num > 0) {
    return chalk.green(formatted);
  }
  if (num < 0) {
    return chalk.red(formatted);
  }
  return chalk.gray(formatted);
}

/**
 * Format future returns for table display
 */
function formatFutureReturns(futureReturns: FutureReturns | undefined): string {
  if (!futureReturns) {
    return '';
  }

  const parts: string[] = [];

  if (futureReturns.day5) {
    parts.push(formatPercentageWithColor(futureReturns.day5.changePercent));
  } else {
    parts.push(chalk.gray('N/A'));
  }

  if (futureReturns.day20) {
    parts.push(formatPercentageWithColor(futureReturns.day20.changePercent));
  } else {
    parts.push(chalk.gray('N/A'));
  }

  if (futureReturns.day60) {
    parts.push(formatPercentageWithColor(futureReturns.day60.changePercent));
  } else {
    parts.push(chalk.gray('N/A'));
  }

  return parts.join(' ');
}

/**
 * Format large numbers with commas
 */
function formatLargeNumber(num: number): string {
  return num.toLocaleString();
}

/**
 * Get screening type display name
 */
function getScreeningTypeDisplay(type: 'rangeBreakFast' | 'rangeBreakSlow'): string {
  switch (type) {
    case 'rangeBreakFast':
      return 'Range Break Fast';
    case 'rangeBreakSlow':
      return 'Range Break Slow';
    default:
      return type;
  }
}

/**
 * Format range break details for table row
 */
function formatRangeBreakDetails(rb: RangeBreakDetails, options: FormatterOptions): string {
  let details = `Break: ${formatPercentage(rb.breakPercentage)}`;

  if (options.verbose) {
    details += `, High: ¥${formatLargeNumber(rb.currentHigh)}, Max: ¥${formatLargeNumber(rb.maxHighInLookback)}`;
  }

  return details;
}

/**
 * Check if any results have future returns
 */
function hasFutureReturns(results: ScreeningResultItem[]): boolean {
  return results.some((r) => r.futureReturns !== undefined);
}

/**
 * Format table row for a single result
 */
function formatTableRow(result: ScreeningResultItem, options: FormatterOptions, showFutureReturns: boolean): string {
  const code = chalk.cyan(padEndVisual(result.stockCode, 8));
  const company = chalk.white(padEndVisual(truncateString(result.companyName, 20), 20));
  const sector33 = chalk.magenta(padEndVisual(truncateString(result.sector33Name || 'N/A', 15), 15));
  const type = chalk.yellow(padEndVisual(getScreeningTypeDisplay(result.screeningType), 16));
  const date = chalk.green(padEndVisual(formatDate(result.matchedDate), 12));

  let volumeRatio = '';
  let details = '';

  if (
    (result.screeningType === 'rangeBreakFast' || result.screeningType === 'rangeBreakSlow') &&
    result.details.rangeBreak
  ) {
    volumeRatio = chalk.cyan(padEndVisual(formatNumber(result.details.rangeBreak.volumeRatio, 2), 8));
    details = formatRangeBreakDetails(result.details.rangeBreak, options);
  }

  let row = code + company + sector33 + type + date + volumeRatio;

  if (showFutureReturns) {
    const futureReturnsStr = formatFutureReturns(result.futureReturns);
    row += padEndVisual(futureReturnsStr, 25);
  }

  row += chalk.gray(details);

  return row;
}

/**
 * Format verbose details line for a result
 */
function formatVerboseDetails(result: ScreeningResultItem): string | null {
  if (result.screeningType === 'rangeBreakFast' && result.details.rangeBreak) {
    const rb = result.details.rangeBreak;
    return chalk.gray(
      `        Volume (EMA): 30D=${formatLargeNumber(rb.avgVolume20Days)}, 120D=${formatLargeNumber(rb.avgVolume100Days)}`
    );
  }

  if (result.screeningType === 'rangeBreakSlow' && result.details.rangeBreak) {
    const rb = result.details.rangeBreak;
    return chalk.gray(
      `        Volume (SMA): 50D=${formatLargeNumber(rb.avgVolume20Days)}, 150D=${formatLargeNumber(rb.avgVolume100Days)}`
    );
  }

  return null;
}

/**
 * Print table header
 */
function printTableHeader(showFutureReturns: boolean): void {
  let header =
    '\n' +
    chalk.bold.white('Code'.padEnd(8)) +
    chalk.bold.white('Company'.padEnd(20)) +
    chalk.bold.white('Sector33'.padEnd(15)) +
    chalk.bold.white('Type'.padEnd(16)) +
    chalk.bold.white('Date'.padEnd(12)) +
    chalk.bold.white('Vol'.padEnd(8));

  if (showFutureReturns) {
    header += chalk.bold.white('+5日   +20日  +60日'.padEnd(25));
  }

  header += chalk.bold.white('Details');

  console.log(header);

  const lineWidth = showFutureReturns ? 135 : 110;
  console.log(chalk.gray('─'.repeat(lineWidth)));
}

/**
 * Print table summary
 */
function printTableSummary(results: ScreeningResultItem[], showFutureReturns: boolean): void {
  const rangeBreakFastCount = results.filter((r) => r.screeningType === 'rangeBreakFast').length;
  const rangeBreakSlowCount = results.filter((r) => r.screeningType === 'rangeBreakSlow').length;

  const lineWidth = showFutureReturns ? 135 : 110;
  console.log(chalk.gray('─'.repeat(lineWidth)));
  console.log(
    chalk.white(`Total: ${results.length} stocks`) +
      chalk.gray(` (Range Break Fast: ${rangeBreakFastCount}, Range Break Slow: ${rangeBreakSlowCount})`)
  );
}

/**
 * Format results as table
 */
function formatAsTable(results: ScreeningResultItem[], options: FormatterOptions): void {
  if (results.length === 0) {
    console.log(chalk.yellow('No results to display'));
    return;
  }

  const showFutureReturns = hasFutureReturns(results);
  printTableHeader(showFutureReturns);

  for (const result of results) {
    console.log(formatTableRow(result, options, showFutureReturns));

    if (options.verbose) {
      const verboseDetails = formatVerboseDetails(result);
      if (verboseDetails) {
        console.log(verboseDetails);
      }
    }
  }

  printTableSummary(results, showFutureReturns);
}

/**
 * Format results as JSON
 */
function formatAsJSON(results: ScreeningResultItem[], options: FormatterOptions): void {
  const output = {
    summary: {
      total: results.length,
      rangeBreakFast: results.filter((r) => r.screeningType === 'rangeBreakFast').length,
      rangeBreakSlow: results.filter((r) => r.screeningType === 'rangeBreakSlow').length,
    },
    results: results.map((result) => ({
      stockCode: result.stockCode,
      companyName: result.companyName,
      scaleCategory: result.scaleCategory,
      sector33Name: result.sector33Name,
      screeningType: result.screeningType,
      matchedDate: result.matchedDate,
      details: options.verbose ? result.details : { rangeBreak: result.details.rangeBreak },
      futureReturns: result.futureReturns,
    })),
  };

  console.log(JSON.stringify(output, null, 2));
}

/**
 * Get CSV headers
 */
function getCSVHeaders(verbose: boolean, includeFutureReturns: boolean): string[] {
  const headers = [
    'StockCode',
    'CompanyName',
    'ScaleCategory',
    'Sector33Name',
    'ScreeningType',
    'MatchedDate',
    'VolumeRatio',
  ];

  if (includeFutureReturns) {
    headers.push('Return5D', 'Return20D', 'Return60D');
  }

  if (verbose) {
    headers.push('BreakPercentage', 'CurrentHigh', 'MaxHighInLookback', 'AvgVolumeShort', 'AvgVolumeLong');
  } else {
    headers.push('Details');
  }

  return headers;
}

/**
 * Format CSV row for range break result
 */
function formatRangeBreakCSVRow(rb: RangeBreakDetails, verbose: boolean): string[] {
  const row = [formatNumber(rb.volumeRatio)];

  if (verbose) {
    row.push(
      formatNumber(rb.breakPercentage),
      formatNumber(rb.currentHigh),
      formatNumber(rb.maxHighInLookback),
      '', // MACD values
      '',
      '',
      formatNumber(rb.avgVolume20Days),
      formatNumber(rb.avgVolume100Days)
    );
  } else {
    row.push(`"Break: ${formatPercentage(rb.breakPercentage)}"`);
  }

  return row;
}

/**
 * Format CSV row for a single result
 */
function formatCSVRow(result: ScreeningResultItem, verbose: boolean, includeFutureReturns: boolean): string {
  const baseRow = [
    `"${result.stockCode}"`,
    `"${result.companyName.replace(/"/g, '""')}"`,
    `"${result.scaleCategory || 'N/A'}"`,
    `"${result.sector33Name || 'N/A'}"`,
    `"${result.screeningType}"`,
    `"${formatDate(result.matchedDate)}"`,
  ];

  let detailsRow: string[] = [];

  if (
    (result.screeningType === 'rangeBreakFast' || result.screeningType === 'rangeBreakSlow') &&
    result.details.rangeBreak
  ) {
    detailsRow = formatRangeBreakCSVRow(result.details.rangeBreak, verbose);
  }

  // Add future returns if included
  if (includeFutureReturns) {
    const fr = result.futureReturns;
    const day5 = fr?.day5?.changePercent?.toFixed(2) ?? '';
    const day20 = fr?.day20?.changePercent?.toFixed(2) ?? '';
    const day60 = fr?.day60?.changePercent?.toFixed(2) ?? '';
    // Insert future returns after volume ratio (index 0 in detailsRow)
    detailsRow.splice(1, 0, day5, day20, day60);
  }

  return [...baseRow, ...detailsRow].join(',');
}

/**
 * Format results as CSV
 */
function formatAsCSV(results: ScreeningResultItem[], options: FormatterOptions): void {
  const includeFutureReturns = hasFutureReturns(results);
  const headers = getCSVHeaders(options.verbose, includeFutureReturns);
  console.log(headers.join(','));

  for (const result of results) {
    console.log(formatCSVRow(result, options.verbose, includeFutureReturns));
  }
}

/**
 * Format results for display
 */
export async function formatResults(results: ScreeningResultItem[], options: FormatterOptions): Promise<void> {
  if (results.length === 0) {
    console.log(chalk.yellow('No results to display'));
    return;
  }

  switch (options.format) {
    case 'json':
      formatAsJSON(results, options);
      break;
    case 'csv':
      formatAsCSV(results, options);
      break;
    default:
      formatAsTable(results, options);
      break;
  }

  // Debug information
  if (options.debug) {
    console.log(chalk.gray('\n🔍 Debug Information:'));
    console.log(chalk.gray(`Results processed: ${results.length}`));
    console.log(chalk.gray(`Format: ${options.format}`));
    console.log(chalk.gray(`Verbose: ${options.verbose}`));
  }
}
