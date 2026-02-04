/**
 * Centralized color scheme utilities for consistent styling across components.
 * These functions return Tailwind CSS classes for financial data visualization.
 */

/**
 * Get color class for positive/negative values.
 * Green for positive, red for negative, muted for zero.
 */
export function getPositiveNegativeColor(value: number): string {
  if (value > 0) return 'text-green-500';
  if (value < 0) return 'text-red-500';
  return 'text-muted-foreground';
}

/**
 * Get color class for ROE (Return on Equity) values.
 * - >= 10%: Green (excellent)
 * - >= 5%: Yellow (acceptable)
 * - < 5%: Red (poor)
 */
export function getRoeColor(value: number): string {
  if (value >= 10) return 'text-green-500';
  if (value >= 5) return 'text-yellow-500';
  return 'text-red-500';
}

/**
 * Get color class for PER (Price-to-Earnings Ratio) values.
 * - < 0 or > 25: Red (overvalued or negative earnings)
 * - <= 15: Green (undervalued)
 * - 15-25: Yellow (fairly valued)
 */
export function getPerColor(value: number): string {
  if (value < 0 || value > 25) return 'text-red-500';
  if (value <= 15) return 'text-green-500';
  return 'text-yellow-500';
}

/**
 * Get color class for PBR (Price-to-Book Ratio) values.
 * - < 1: Green (undervalued)
 * - <= 2: Yellow (fairly valued)
 * - > 2: Red (overvalued)
 */
export function getPbrColor(value: number): string {
  if (value < 1) return 'text-green-500';
  if (value <= 2) return 'text-yellow-500';
  return 'text-red-500';
}

/**
 * Get color class for cash flow values.
 * Positive is good, negative is bad, zero is neutral.
 */
export function getCashFlowColor(value: number): string {
  if (value > 0) return 'text-green-500';
  if (value < 0) return 'text-red-500';
  return 'text-muted-foreground';
}

/**
 * Get color class for return percentage values with dark mode support.
 * Green for positive, red for negative, muted for zero/null.
 */
export function getReturnColor(value: number | null | undefined): string {
  if (value === null || value === undefined) return 'text-muted-foreground';
  if (value > 0) return 'text-green-600 dark:text-green-400';
  if (value < 0) return 'text-red-600 dark:text-red-400';
  return 'text-muted-foreground';
}

/**
 * Get color class for FCF Yield values (higher is better).
 * - >= 5%: Green (excellent)
 * - >= 2%: Yellow (acceptable)
 * - < 2%: Red (poor)
 * - Negative: Red
 */
export function getFcfYieldColor(value: number): string {
  if (value >= 5) return 'text-green-500';
  if (value >= 2) return 'text-yellow-500';
  return 'text-red-500';
}

/**
 * Get color class for FCF Margin values (higher is better).
 * - >= 10%: Green (excellent)
 * - >= 5%: Yellow (acceptable)
 * - < 5%: Red (poor)
 * - Negative: Red
 */
export function getFcfMarginColor(value: number): string {
  if (value >= 10) return 'text-green-500';
  if (value >= 5) return 'text-yellow-500';
  return 'text-red-500';
}

/**
 * Color scheme types for fundamental metrics
 */
export type FundamentalColorScheme = 'roe' | 'per' | 'pbr' | 'cashFlow' | 'fcfYield' | 'fcfMargin' | 'neutral';

/**
 * Get color class for fundamental metrics based on scheme type.
 * Dispatches to the appropriate color function based on the scheme.
 */
export function getFundamentalColor(value: number | null, scheme: FundamentalColorScheme): string {
  if (value === null) return 'text-muted-foreground';
  if (scheme === 'neutral') return 'text-foreground';

  switch (scheme) {
    case 'roe':
      return getRoeColor(value);
    case 'per':
      return getPerColor(value);
    case 'pbr':
      return getPbrColor(value);
    case 'cashFlow':
      return getCashFlowColor(value);
    case 'fcfYield':
      return getFcfYieldColor(value);
    case 'fcfMargin':
      return getFcfMarginColor(value);
    default:
      return 'text-foreground';
  }
}
