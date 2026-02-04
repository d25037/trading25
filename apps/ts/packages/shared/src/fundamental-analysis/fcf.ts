/**
 * Free Cash Flow (FCF) Calculation Module
 * Calculates Simple FCF, FCF Yield, and FCF Margin
 * Using JQuants API v2 field names
 */

import type { JQuantsStatement } from '../types/jquants';

/**
 * Calculate Simple Free Cash Flow
 * Simple FCF = Operating Cash Flow + Investing Cash Flow
 *
 * Note: CFI is typically negative (investments), so adding CFO + CFI gives FCF
 *
 * @param cfo Cash Flow from Operating Activities
 * @param cfi Cash Flow from Investing Activities
 * @returns Simple FCF or null if calculation not possible
 */
export function calculateSimpleFCF(cfo: number | null, cfi: number | null): number | null {
  if (cfo === null || cfi === null) {
    return null;
  }
  return cfo + cfi;
}

/**
 * Calculate FCF Yield
 * FCF Yield = (FCF / Market Cap) × 100
 *
 * Market Cap = Stock Price × (Shares Outstanding - Treasury Shares)
 *
 * @param fcf Free Cash Flow
 * @param stockPrice Current stock price
 * @param sharesOutstanding Total shares outstanding (including treasury shares)
 * @param treasuryShares Treasury shares (optional, defaults to 0)
 * @returns FCF Yield percentage or null if calculation not possible
 */
export function calculateFCFYield(
  fcf: number | null,
  stockPrice: number | null,
  sharesOutstanding: number | null,
  treasuryShares?: number | null
): number | null {
  if (fcf === null || stockPrice === null || sharesOutstanding === null || stockPrice <= 0) {
    return null;
  }

  const actualShares = sharesOutstanding - (treasuryShares ?? 0);
  if (actualShares <= 0) {
    return null;
  }

  // marketCap is guaranteed positive since stockPrice > 0 and actualShares > 0
  const marketCap = stockPrice * actualShares;
  return (fcf / marketCap) * 100;
}

/**
 * Calculate FCF Margin
 * FCF Margin = (FCF / Net Sales) × 100
 *
 * @param fcf Free Cash Flow
 * @param sales Net Sales
 * @returns FCF Margin percentage or null if calculation not possible
 */
export function calculateFCFMargin(fcf: number | null, sales: number | null): number | null {
  if (fcf === null || sales === null || sales <= 0) {
    return null;
  }
  return (fcf / sales) * 100;
}

/**
 * Get Cash Flow from Operating Activities from statement
 * @param statement JQuants financial statement
 * @returns Operating cash flow or null
 */
export function getCashFlowOperating(statement: JQuantsStatement): number | null {
  return statement.CFO ?? null;
}

/**
 * Get Cash Flow from Investing Activities from statement
 * @param statement JQuants financial statement
 * @returns Investing cash flow or null
 */
export function getCashFlowInvesting(statement: JQuantsStatement): number | null {
  return statement.CFI ?? null;
}

/**
 * Get Cash Flow from Financing Activities from statement
 * @param statement JQuants financial statement
 * @returns Financing cash flow or null
 */
export function getCashFlowFinancing(statement: JQuantsStatement): number | null {
  return statement.CFF ?? null;
}

/**
 * Get Cash and Cash Equivalents from statement
 * @param statement JQuants financial statement
 * @returns Cash and equivalents or null
 */
export function getCashAndEquivalents(statement: JQuantsStatement): number | null {
  return statement.CashEq ?? null;
}

/**
 * Get Shares Outstanding at Fiscal Year End from statement
 * @param statement JQuants financial statement
 * @returns Shares outstanding or null
 */
export function getSharesOutstanding(statement: JQuantsStatement): number | null {
  return statement.ShOutFY ?? null;
}

/**
 * Get Treasury Shares at Fiscal Year End from statement
 * @param statement JQuants financial statement
 * @returns Treasury shares or null
 */
export function getTreasuryShares(statement: JQuantsStatement): number | null {
  return statement.TrShFY ?? null;
}
