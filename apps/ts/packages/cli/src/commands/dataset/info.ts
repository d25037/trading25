/**
 * Dataset - Info Command
 * Display dataset information via API
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';
import { ApiClient, type DatasetInfoResponse } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { displayFooter, displayHeader, displayKeyValue, displaySection } from '../../utils/display-helpers.js';
import { CLIError, CLIValidationError, DATASET_TIPS, handleCommandError } from '../../utils/error-handling.js';
import { formatAvailability, formatBytes, formatCoverage, logDebug } from '../../utils/format-helpers.js';

/**
 * Format creation date safely, handling invalid date strings
 */
function formatCreatedAt(createdAt: string | null): string {
  if (!createdAt) return chalk.gray('Unknown');

  const date = new Date(createdAt);
  if (Number.isNaN(date.getTime())) return chalk.gray('Invalid date');

  return chalk.cyan(date.toLocaleString());
}

/**
 * Display snapshot metadata section (preset and creation time)
 */
function displaySnapshotSection(info: DatasetInfoResponse): void {
  displaySection('📸', 'Snapshot');
  displayKeyValue(
    'Preset',
    info.snapshot.preset ? chalk.cyan(info.snapshot.preset) : chalk.gray('Unknown (legacy dataset)')
  );
  displayKeyValue('Created', formatCreatedAt(info.snapshot.createdAt));
}

/**
 * Display database metadata section
 */
function displayDatabaseSection(info: DatasetInfoResponse): void {
  displaySection('💾', 'Database');
  displayKeyValue('Name', chalk.cyan(info.name));
  displayKeyValue('Path', chalk.gray(info.path));
  displayKeyValue('Size', chalk.yellow(formatBytes(info.fileSize)));
  displayKeyValue('Last Modified', chalk.cyan(info.lastModified));
}

/**
 * Display data summary section
 */
function displayDataSummarySection(info: DatasetInfoResponse): void {
  displaySection('📊', 'Data Summary');
  displayKeyValue('Total Stocks', chalk.yellow(info.stats.totalStocks.toLocaleString()));
  displayKeyValue('Total Quotes', chalk.yellow(info.stats.totalQuotes.toLocaleString()));
  displayKeyValue('Date Range', `${chalk.cyan(info.stats.dateRange.from)} to ${chalk.cyan(info.stats.dateRange.to)}`);
}

/**
 * Display data availability section with coverage details
 */
function displayAvailabilitySection(info: DatasetInfoResponse): void {
  const dc = info.validation.details?.dataCoverage;

  displaySection('📈', 'Data Availability');
  displayKeyValue('TOPIX Data', formatAvailability(info.stats.hasTOPIXData));
  displayKeyValue('Sector Indices', formatAvailability(info.stats.hasSectorData));

  // Stock quotes coverage
  const quotesStatus = dc ? formatCoverage(dc.stocksWithQuotes, dc.totalStocks) : chalk.gray('N/A');
  displayKeyValue('Stock Quotes', quotesStatus);

  // Financial statements (only show coverage if data exists)
  if (info.stats.hasStatementsData && dc) {
    displayKeyValue('Financial Statements', formatCoverage(dc.stocksWithStatements, dc.totalStocks));
  } else {
    displayKeyValue('Financial Statements', formatAvailability(info.stats.hasStatementsData));
  }

  // Margin data (only show coverage if data exists)
  if (info.stats.hasMarginData && dc) {
    displayKeyValue('Margin Data', formatCoverage(dc.stocksWithMargin, dc.totalStocks));
  } else {
    displayKeyValue('Margin Data', formatAvailability(info.stats.hasMarginData));
  }
}

/**
 * Get chalk color function based on coverage percentage
 */
function getCoverageColor(percentage: number): typeof chalk.green {
  if (percentage >= 90) return chalk.green;
  if (percentage > 0) return chalk.yellow;
  return chalk.red;
}

/**
 * Format field coverage with count, total, and percentage
 */
function formatFieldCoverage(count: number, total: number, suffix?: string): string {
  const pct = total > 0 ? ((count / total) * 100).toFixed(1) : '0.0';
  const color = getCoverageColor(Number(pct));
  const base = `${color(count.toLocaleString())}/${total.toLocaleString()} (${pct}%)`;
  return suffix ? `${base} ${chalk.dim(suffix)}` : base;
}

/**
 * Field definition for coverage display
 */
interface FieldDef {
  label: string;
  key: keyof NonNullable<DatasetInfoResponse['stats']['statementsFieldCoverage']>;
  denominator: 'total' | 'totalFY' | 'totalHalf';
  suffix?: string;
}

/**
 * Section definition for grouped field display
 */
interface SectionDef {
  title: string;
  fields: FieldDef[];
  requiresExtended?: boolean;
  requiresCashFlow?: boolean;
}

/**
 * Field coverage section definitions
 */
const FIELD_COVERAGE_SECTIONS: SectionDef[] = [
  {
    title: 'Core Fields (all periods)',
    fields: [
      { label: 'EPS', key: 'earningsPerShare', denominator: 'total' },
      { label: 'Profit', key: 'profit', denominator: 'total' },
      { label: 'Equity', key: 'equity', denominator: 'total' },
    ],
  },
  {
    title: 'Core Fields (FY only)',
    fields: [{ label: 'Next Year Forecast EPS', key: 'nextYearForecastEps', denominator: 'totalFY' }],
  },
  {
    title: 'Extended Fields (all periods)',
    requiresExtended: true,
    fields: [
      { label: 'Sales', key: 'sales', denominator: 'total' },
      { label: 'Operating Profit', key: 'operatingProfit', denominator: 'total' },
      { label: 'Forecast EPS', key: 'forecastEps', denominator: 'total' },
      { label: 'Ordinary Profit', key: 'ordinaryProfit', denominator: 'total', suffix: '(J-GAAP only)' },
    ],
  },
  {
    title: 'Extended Fields (FY+2Q only)',
    requiresExtended: true,
    fields: [
      { label: 'Operating Cash Flow', key: 'operatingCashFlow', denominator: 'totalHalf' },
      { label: 'BPS', key: 'bps', denominator: 'totalHalf' },
      { label: 'Dividend (FY)', key: 'dividendFY', denominator: 'totalFY' },
    ],
  },
  {
    title: 'Cash Flow Extended Fields (FY+2Q only)',
    requiresCashFlow: true,
    fields: [
      { label: 'Investing Cash Flow', key: 'investingCashFlow', denominator: 'totalHalf' },
      { label: 'Financing Cash Flow', key: 'financingCashFlow', denominator: 'totalHalf' },
      { label: 'Cash & Equivalents', key: 'cashAndEquivalents', denominator: 'totalHalf' },
    ],
  },
  {
    title: 'Cash Flow Extended Fields (all periods)',
    requiresCashFlow: true,
    fields: [
      { label: 'Total Assets', key: 'totalAssets', denominator: 'total' },
      { label: 'Shares Outstanding', key: 'sharesOutstanding', denominator: 'total' },
      { label: 'Treasury Shares', key: 'treasuryShares', denominator: 'total' },
    ],
  },
];

/**
 * Display statements field coverage section
 * Shows which financial metrics are available with appropriate denominators
 */
function displayStatementsFieldCoverageSection(info: DatasetInfoResponse): void {
  const fc = info.stats.statementsFieldCoverage;
  if (!fc || fc.total === 0) return;

  displaySection('📋', 'Statements Field Coverage');
  displayKeyValue(
    'Total Records',
    `${chalk.cyan(fc.total.toLocaleString())} (FY: ${fc.totalFY}, FY+2Q: ${fc.totalHalf})`
  );

  for (const section of FIELD_COVERAGE_SECTIONS) {
    if (section.requiresExtended && !fc.hasExtendedFields) {
      console.log(chalk.dim(`  -- Extended Fields --`));
      console.log(chalk.red('  (Schema outdated - fields not available)'));
      return;
    }

    if (section.requiresCashFlow && !fc.hasCashFlowFields) {
      console.log(chalk.dim(`  -- Cash Flow Extended Fields --`));
      console.log(chalk.yellow('  (Schema version 1 - CF fields not available)'));
      return;
    }

    console.log(chalk.dim(`  -- ${section.title} --`));
    for (const field of section.fields) {
      const count = fc[field.key] as number;
      const total = fc[field.denominator];
      displayKeyValue(field.label, formatFieldCoverage(count, total, field.suffix));
    }
  }
}

/**
 * Display validation errors and warnings section
 */
function displayValidationSection(info: DatasetInfoResponse): void {
  const { validation } = info;

  if (validation.errors.length === 0 && validation.warnings.length === 0) {
    displaySection('✅', 'Validation');
    console.log(chalk.green('  No issues found'));
    return;
  }

  displaySection(validation.isValid ? '⚠️' : '❌', 'Validation');

  for (const error of validation.errors) {
    console.log(chalk.red(`  ✗ ${error}`));
  }
  for (const warning of validation.warnings) {
    console.log(chalk.yellow(`  ⚠ ${warning}`));
  }
}

/**
 * Display dataset information with validation
 */
function displayDatasetInfo(info: DatasetInfoResponse): void {
  displayHeader('Dataset Information');

  displaySnapshotSection(info);
  displayDatabaseSection(info);
  displayDataSummarySection(info);
  displayAvailabilitySection(info);
  displayStatementsFieldCoverageSection(info);
  displayValidationSection(info);

  displayFooter();
}

/**
 * Info command definition
 */
export const infoCommand = define({
  name: 'info',
  description: 'Show dataset snapshot details including preset, creation date, and validation',
  args: {
    name: {
      type: 'positional',
      description: 'Dataset filename (within XDG datasets directory)',
    },
    json: {
      type: 'boolean',
      description: 'Output as JSON format',
    },
    strict: {
      type: 'boolean',
      description: 'Exit with code 1 if validation fails (for CI)',
    },
    debug: {
      type: 'boolean',
      description: 'Enable detailed output for debugging',
    },
  },
  examples: `
# Show dataset information
${CLI_NAME} dataset info prime.db

# Output as JSON
${CLI_NAME} dataset info prime.db --json

# CI mode (exit 1 on validation failure)
${CLI_NAME} dataset info prime.db --strict
  `.trim(),
  run: async (ctx) => {
    const { name: datasetName, json, strict, debug } = ctx.values;
    const isDebug = debug ?? false;
    const spinner = ora('Fetching dataset information...').start();

    if (!datasetName) {
      spinner.fail(chalk.red('Dataset name is required'));
      throw new CLIValidationError('Dataset name is required');
    }

    logDebug(isDebug, `Dataset name: ${datasetName}`);

    try {
      const apiClient = new ApiClient();
      const datasetClient = apiClient.dataset;

      spinner.text = 'Retrieving dataset statistics...';
      const info = await datasetClient.getDatasetInfo(datasetName);

      spinner.stop();

      logDebug(isDebug, 'Response received');
      logDebug(isDebug, `Total stocks: ${info.stats.totalStocks}`);

      if (json) {
        console.log(JSON.stringify(info, null, 2));
      } else {
        displayDatasetInfo(info);
      }

      // Exit with code 1 if strict mode and validation failed
      if (strict && !info.validation.isValid) {
        throw new CLIError('Dataset validation failed in strict mode', 1, true);
      }
    } catch (error) {
      handleCommandError(error, spinner, {
        failMessage: 'Failed to get dataset information',
        debug: isDebug,
        tips: DATASET_TIPS.info,
      });
    }
  },
});
