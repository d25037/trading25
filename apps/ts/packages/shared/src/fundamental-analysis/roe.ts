import { getErrorMessage } from '../errors';
import type { JQuantsStatement } from '../types/jquants';
import { logger } from '../utils/logger';
import type { ROECalculationOptions, ROEMetadata, ROEResult } from './types';
import { ROECalculationError } from './types';
import { isQuarterlyPeriod, normalizePeriodType } from './utils';

/**
 * Calculates Return on Equity (ROE) from JQuants financial statement data
 *
 * ROE = (Net Profit / Shareholders' Equity) × 100
 *
 * @param statement JQuants financial statement data
 * @param options Calculation options
 * @returns ROE calculation result
 * @throws ROECalculationError when calculation is not possible
 */
export function calculateROE(statement: JQuantsStatement, options: ROECalculationOptions = {}): ROEResult {
  const { annualize = true, preferConsolidated = true, minEquityThreshold = 1000 } = options;
  const normalizedPeriodType = normalizePeriodType(statement.CurPerType);

  // Validate input
  if (!statement) {
    throw new ROECalculationError('Statement data is required', 'unknown');
  }

  // Extract profit and equity values
  const { netProfit, equity } = extractFinancialValues(statement, preferConsolidated);

  // Validate required fields
  if (netProfit === null || netProfit === undefined) {
    throw new ROECalculationError(
      'Net profit data is not available',
      statement.Code,
      statement,
      'profit_not_available'
    );
  }

  if (equity === null || equity === undefined) {
    throw new ROECalculationError('Equity data is not available', statement.Code, statement, 'equity_not_available');
  }

  // Validate equity threshold
  if (Math.abs(equity) < minEquityThreshold) {
    throw new ROECalculationError(
      `Equity value (${equity}) is below minimum threshold (${minEquityThreshold})`,
      statement.Code,
      statement,
      'equity_below_threshold'
    );
  }

  // Handle zero or negative equity
  if (equity <= 0) {
    throw new ROECalculationError(
      `Invalid equity value: ${equity}. ROE calculation requires positive equity.`,
      statement.Code,
      statement,
      'invalid_equity'
    );
  }

  // Calculate ROE
  let adjustedNetProfit = netProfit;

  // Annualize quarterly data if requested
  if (annualize && isQuarterlyPeriod(statement.CurPerType)) {
    adjustedNetProfit = annualizeQuarterlyProfit(netProfit, normalizedPeriodType ?? statement.CurPerType);
  }

  const roe = (adjustedNetProfit / equity) * 100;

  // Create metadata
  const metadata: ROEMetadata = {
    code: statement.Code,
    periodType: normalizedPeriodType ?? statement.CurPerType,
    periodStart: statement.CurPerSt,
    periodEnd: statement.CurPerEn,
    documentType: statement.DocType,
    isConsolidated: isConsolidatedDocument(statement.DocType),
    accountingStandard: extractAccountingStandard(statement.DocType),
    isAnnualized: annualize && isQuarterlyPeriod(normalizedPeriodType ?? statement.CurPerType),
  };

  return {
    roe,
    netProfit: adjustedNetProfit,
    equity,
    statement,
    metadata,
  };
}

/**
 * Calculates ROE for multiple financial statements
 * Automatically selects the most recent annual (FY) statement for each company
 */
export function calculateROEBatch(statements: JQuantsStatement[], options: ROECalculationOptions = {}): ROEResult[] {
  if (!statements || statements.length === 0) {
    return [];
  }

  // Group by company and select most recent FY statement
  const companyStatements = new Map<string, JQuantsStatement>();

  for (const statement of statements) {
    const code = statement.Code;
    const current = companyStatements.get(code);

    // Prefer FY statements over quarterly
    if (!current || shouldPreferStatement(statement, current)) {
      companyStatements.set(code, statement);
    }
  }

  // Calculate ROE for each selected statement
  const results: ROEResult[] = [];

  for (const statement of companyStatements.values()) {
    try {
      const result = calculateROE(statement, options);
      results.push(result);
    } catch (error) {
      logger.debug(`Failed to calculate ROE for ${statement.Code}`, { error: getErrorMessage(error) });
    }
  }

  return results.sort((a, b) => b.roe - a.roe); // Sort by ROE descending
}

/**
 * Extract profit and equity values, preferring consolidated data
 */
function extractFinancialValues(
  statement: JQuantsStatement,
  preferConsolidated: boolean
): { netProfit: number | null; equity: number | null } {
  let netProfit: number | null = null;
  let equity: number | null = null;

  if (preferConsolidated) {
    // Try consolidated data first (NP = Net Profit, Eq = Equity)
    netProfit = statement.NP;
    equity = statement.Eq;

    // Fall back to non-consolidated if consolidated is not available
    if ((netProfit === null || netProfit === undefined) && statement.NCNP !== null) {
      netProfit = statement.NCNP;
    }
    if ((equity === null || equity === undefined) && statement.NCEq !== null) {
      equity = statement.NCEq;
    }
  } else {
    // Try non-consolidated data first
    netProfit = statement.NCNP ?? statement.NP;
    equity = statement.NCEq ?? statement.Eq;
  }

  return { netProfit, equity };
}

/**
 * Annualize quarterly profit figures
 */
function annualizeQuarterlyProfit(quarterlyProfit: number, periodType: string): number {
  const normalized = normalizePeriodType(periodType);
  switch (normalized) {
    case '1Q':
      return quarterlyProfit * 4; // 1Q × 4
    case '2Q':
      return quarterlyProfit * 2; // H1 × 2 (半期 × 2)
    case '3Q':
      return quarterlyProfit * (4 / 3); // 1Q-3Q × 4/3
    default:
      return quarterlyProfit;
  }
}

/**
 * Check if document type represents consolidated financials
 */
function isConsolidatedDocument(documentType: string): boolean {
  return documentType.toLowerCase().includes('consolidated');
}

/**
 * Extract accounting standard from document type
 */
function extractAccountingStandard(documentType: string): string | null {
  const lower = documentType.toLowerCase();

  if (lower.includes('ifrs')) {
    return 'IFRS';
  }
  if (lower.includes('us') && lower.includes('gaap')) {
    return 'US GAAP';
  }
  if (lower.includes('jp') || lower.includes('japanese')) {
    return 'JGAAP';
  }

  // Default to JGAAP for most Japanese companies
  return 'JGAAP';
}

/**
 * Determine which statement to prefer when multiple are available
 */
function shouldPreferStatement(newStatement: JQuantsStatement, currentStatement: JQuantsStatement): boolean {
  // Prefer FY over quarterly
  if (newStatement.CurPerType === 'FY' && currentStatement.CurPerType !== 'FY') {
    return true;
  }
  if (newStatement.CurPerType !== 'FY' && currentStatement.CurPerType === 'FY') {
    return false;
  }

  // If both are the same period type, prefer more recent
  const newDate = new Date(newStatement.CurPerEn);
  const currentDate = new Date(currentStatement.CurPerEn);

  return newDate > currentDate;
}

/**
 * Utility function to format ROE result for display
 */
export function formatROEResult(result: ROEResult): string {
  const { roe, metadata } = result;
  const annualizedNote = metadata.isAnnualized ? ' (annualized)' : '';

  return `${metadata.code}: ROE ${roe.toFixed(2)}%${annualizedNote} (${metadata.periodType} ${metadata.periodEnd})`;
}

/**
 * Validate if a statement has sufficient data for ROE calculation
 */
export function canCalculateROE(statement: JQuantsStatement, preferConsolidated = true): boolean {
  try {
    const { netProfit, equity } = extractFinancialValues(statement, preferConsolidated);
    return netProfit !== null && netProfit !== undefined && equity !== null && equity !== undefined && equity > 0;
  } catch {
    return false;
  }
}
