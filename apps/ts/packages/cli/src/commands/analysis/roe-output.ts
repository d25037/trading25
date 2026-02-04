/**
 * ROE Output Functions
 * Table and CSV output formatting for ROE analysis
 */

import chalk from 'chalk';
import type { ROEResultItem } from '../../utils/api-client';
import type { OutputManager } from '../../utils/OutputManager';

/**
 * Output results in table format
 */
export function outputTable(results: ROEResultItem[], _output: OutputManager): void {
  console.log();
  console.log(chalk.bold('ROE Analysis Results'));
  console.log('═'.repeat(80));

  renderTableHeader();
  renderTableRows(results);
  renderTableFooter();
  renderSummaryStatistics(results);
  renderLegend();
}

/**
 * Output results in CSV format
 */
export function outputCSV(results: ROEResultItem[]): void {
  console.log('Code,ROE,NetProfit,Equity,Period,EndDate,Consolidated,Standard,Annualized');

  for (const result of results) {
    console.log(
      [
        result.metadata.code,
        result.roe.toFixed(2),
        result.netProfit,
        result.equity,
        result.metadata.periodType,
        result.metadata.periodEnd,
        result.metadata.isConsolidated,
        result.metadata.accountingStandard || 'JGAAP',
        result.metadata.isAnnualized || false,
      ].join(',')
    );
  }
}

/**
 * Render table header
 */
function renderTableHeader(): void {
  console.log(
    chalk.bold.cyan('Code'.padEnd(8)) +
      chalk.bold.cyan('ROE %'.padEnd(10)) +
      chalk.bold.cyan('Period'.padEnd(8)) +
      chalk.bold.cyan('End Date'.padEnd(12)) +
      chalk.bold.cyan('Type'.padEnd(6)) +
      chalk.bold.cyan('Standard'.padEnd(8)) +
      chalk.bold.cyan('Notes'.padEnd(15))
  );
  console.log('─'.repeat(80));
}

/**
 * Render table rows
 */
function renderTableRows(results: ROEResultItem[]): void {
  for (const result of results) {
    const notes = buildNotes(result);
    const roeColor = getROEColor(result.roe);

    console.log(
      chalk.bold(result.metadata.code.padEnd(8)) +
        roeColor(result.roe.toFixed(2).padEnd(10)) +
        result.metadata.periodType.padEnd(8) +
        result.metadata.periodEnd.padEnd(12) +
        (result.metadata.isConsolidated ? 'Con.' : 'Non.').padEnd(6) +
        (result.metadata.accountingStandard || 'JGAAP').padEnd(8) +
        notes.join(', ').padEnd(15)
    );
  }
}

/**
 * Render table footer
 */
function renderTableFooter(): void {
  console.log('─'.repeat(80));
  console.log();
}

/**
 * Render summary statistics
 */
function renderSummaryStatistics(results: ROEResultItem[]): void {
  const avgROE = results.reduce((sum, r) => sum + r.roe, 0) / results.length;
  const maxROE = Math.max(...results.map((r) => r.roe));
  const minROE = Math.min(...results.map((r) => r.roe));

  console.log(chalk.bold('Summary Statistics:'));
  console.log(`Average ROE: ${chalk.cyan(avgROE.toFixed(2))}%`);
  console.log(
    `Highest ROE: ${chalk.green(maxROE.toFixed(2))}% (${results.find((r) => r.roe === maxROE)?.metadata.code})`
  );
  console.log(
    `Lowest ROE:  ${chalk.red(minROE.toFixed(2))}% (${results.find((r) => r.roe === minROE)?.metadata.code})`
  );
  console.log(`Companies:   ${results.length}`);
  console.log();
}

/**
 * Render legend
 */
function renderLegend(): void {
  console.log(chalk.dim('Legend:'));
  console.log(chalk.dim('  Ann. = Annualized quarterly data'));
  console.log(chalk.dim('  Non-Con. = Non-consolidated financials'));
  console.log(chalk.dim('  Con. = Consolidated, Non. = Non-consolidated'));
  console.log(chalk.dim('  Colors: Green ≥15%, Yellow ≥10%, White ≥5%, Red <5%'));
}

/**
 * Build notes array for a result
 */
function buildNotes(result: ROEResultItem): string[] {
  const notes = [];

  if (result.metadata.isAnnualized) {
    notes.push('Ann.');
  }
  if (!result.metadata.isConsolidated) {
    notes.push('Non-Con.');
  }

  return notes;
}

/**
 * Get color function for ROE value
 */
function getROEColor(roe: number): (text: string) => string {
  if (roe >= 15) return chalk.green;
  if (roe >= 10) return chalk.yellow;
  if (roe >= 5) return chalk.white;
  return chalk.red;
}
