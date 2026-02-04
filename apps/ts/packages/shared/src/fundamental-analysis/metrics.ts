/**
 * Financial Metrics Calculation Module
 * Calculates EPS, BPS, PER, PBR, ROA, Operating Margin, and Net Margin
 * Using JQuants API v2 field names
 */

import type { JQuantsStatement } from '../types/jquants';

/**
 * Calculate Earnings Per Share (EPS)
 * Returns the EPS from the statement, preferring consolidated or non-consolidated based on preference
 */
export function calculateEPS(statement: JQuantsStatement, preferConsolidated = true): number | null {
  if (preferConsolidated) {
    // Try consolidated first (EPS), then fall back to non-consolidated (NCEPS)
    return statement.EPS ?? statement.NCEPS ?? null;
  }
  // Try non-consolidated first, then fall back to consolidated
  return statement.NCEPS ?? statement.EPS ?? null;
}

/**
 * Calculate Diluted Earnings Per Share
 */
export function calculateDilutedEPS(statement: JQuantsStatement): number | null {
  return statement.DEPS ?? null;
}

/**
 * Calculate Book Value Per Share (BPS)
 * Returns the BPS from the statement, preferring consolidated or non-consolidated based on preference
 */
export function calculateBPS(statement: JQuantsStatement, preferConsolidated = true): number | null {
  if (preferConsolidated) {
    // Try consolidated first (BPS), then fall back to non-consolidated (NCBPS)
    return statement.BPS ?? statement.NCBPS ?? null;
  }
  // Try non-consolidated first, then fall back to consolidated
  return statement.NCBPS ?? statement.BPS ?? null;
}

/**
 * Calculate Price to Earnings Ratio (PER)
 * PER = Stock Price / EPS
 * @param eps Earnings per share
 * @param stockPrice Stock price at the time
 * @returns PER value or null if calculation not possible
 */
export function calculatePER(eps: number | null, stockPrice: number | null): number | null {
  if (eps === null || stockPrice === null || eps === 0) {
    return null;
  }
  // Handle negative EPS (company in loss) - still calculate PER but it will be negative
  return stockPrice / eps;
}

/**
 * Calculate Price to Book Ratio (PBR)
 * PBR = Stock Price / BPS
 * @param bps Book value per share
 * @param stockPrice Stock price at the time
 * @returns PBR value or null if calculation not possible
 */
export function calculatePBR(bps: number | null, stockPrice: number | null): number | null {
  if (bps === null || stockPrice === null || bps <= 0) {
    return null;
  }
  return stockPrice / bps;
}

/**
 * Calculate Return on Assets (ROA)
 * ROA = (Net Profit / Total Assets) × 100
 */
export function calculateROA(statement: JQuantsStatement, preferConsolidated = true): number | null {
  const profit = preferConsolidated ? (statement.NP ?? statement.NCNP) : (statement.NCNP ?? statement.NP);

  const totalAssets = preferConsolidated ? (statement.TA ?? statement.NCTA) : (statement.NCTA ?? statement.TA);

  if (
    profit === null ||
    profit === undefined ||
    totalAssets === null ||
    totalAssets === undefined ||
    totalAssets <= 0
  ) {
    return null;
  }

  return (profit / totalAssets) * 100;
}

/**
 * Calculate Operating Profit Margin
 * Operating Margin = (Operating Profit / Net Sales) × 100
 */
export function calculateOperatingMargin(statement: JQuantsStatement, preferConsolidated = true): number | null {
  const operatingProfit = preferConsolidated ? (statement.OP ?? statement.NCOP) : (statement.NCOP ?? statement.OP);

  const netSales = preferConsolidated ? (statement.Sales ?? statement.NCSales) : (statement.NCSales ?? statement.Sales);

  if (
    operatingProfit === null ||
    operatingProfit === undefined ||
    netSales === null ||
    netSales === undefined ||
    netSales <= 0
  ) {
    return null;
  }

  return (operatingProfit / netSales) * 100;
}

/**
 * Calculate Net Profit Margin
 * Net Margin = (Net Profit / Net Sales) × 100
 */
export function calculateNetMargin(statement: JQuantsStatement, preferConsolidated = true): number | null {
  const profit = preferConsolidated ? (statement.NP ?? statement.NCNP) : (statement.NCNP ?? statement.NP);

  const netSales = preferConsolidated ? (statement.Sales ?? statement.NCSales) : (statement.NCSales ?? statement.Sales);

  if (profit === null || profit === undefined || netSales === null || netSales === undefined || netSales <= 0) {
    return null;
  }

  return (profit / netSales) * 100;
}

/**
 * Get net profit from statement
 */
export function getNetProfit(statement: JQuantsStatement, preferConsolidated = true): number | null {
  if (preferConsolidated) {
    return statement.NP ?? statement.NCNP ?? null;
  }
  return statement.NCNP ?? statement.NP ?? null;
}

/**
 * Get equity from statement
 */
export function getEquity(statement: JQuantsStatement, preferConsolidated = true): number | null {
  if (preferConsolidated) {
    return statement.Eq ?? statement.NCEq ?? null;
  }
  return statement.NCEq ?? statement.Eq ?? null;
}

/**
 * Get total assets from statement
 */
export function getTotalAssets(statement: JQuantsStatement, preferConsolidated = true): number | null {
  if (preferConsolidated) {
    return statement.TA ?? statement.NCTA ?? null;
  }
  return statement.NCTA ?? statement.TA ?? null;
}

/**
 * Get net sales from statement
 */
export function getNetSales(statement: JQuantsStatement, preferConsolidated = true): number | null {
  if (preferConsolidated) {
    return statement.Sales ?? statement.NCSales ?? null;
  }
  return statement.NCSales ?? statement.Sales ?? null;
}

/**
 * Get operating profit from statement
 */
export function getOperatingProfit(statement: JQuantsStatement, preferConsolidated = true): number | null {
  if (preferConsolidated) {
    return statement.OP ?? statement.NCOP ?? null;
  }
  return statement.NCOP ?? statement.OP ?? null;
}

/**
 * Check if statement has consolidated data
 */
export function hasConsolidatedData(statement: JQuantsStatement): boolean {
  return statement.NP !== null || statement.Eq !== null || statement.TA !== null || statement.Sales !== null;
}

/**
 * Determine if the statement is consolidated based on document type
 */
export function isConsolidatedStatement(statement: JQuantsStatement): boolean {
  const docType = statement.DocType.toLowerCase();
  // Non-consolidated statements typically have "非連結" or "NonConsolidated" in the document type
  // If neither is present, check if consolidated fields have data
  if (docType.includes('非連結') || docType.includes('nonconsolidated')) {
    return false;
  }
  if (docType.includes('連結') || docType.includes('consolidated')) {
    return true;
  }
  // Default: check if consolidated data is available
  return hasConsolidatedData(statement);
}

/**
 * Extract accounting standard from document type
 */
export function getAccountingStandard(statement: JQuantsStatement): string | null {
  const docType = statement.DocType.toLowerCase();

  if (docType.includes('ifrs')) {
    return 'IFRS';
  }
  if (docType.includes('us') && docType.includes('gaap')) {
    return 'US GAAP';
  }
  if (docType.includes('jp') || docType.includes('japanese')) {
    return 'JGAAP';
  }

  // Default to JGAAP for most Japanese companies
  return 'JGAAP';
}
