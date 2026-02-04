/**
 * Financial Data Utilities
 *
 * Common utilities for filtering financial data, validating metrics,
 * and handling period types across JQuants API and Dataset data sources.
 */

/**
 * Safely convert a value to number or null.
 * JQuants API sometimes returns large numbers as strings,
 * and forecast data may contain empty strings.
 */
export function toNumberOrNull(value: unknown): number | null {
  if (value === null || value === undefined || value === '') return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  if (typeof value === 'string') {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

/**
 * Check if period type is FY (full year / annual)
 */
export function isFiscalYear(periodType: string | null | undefined): boolean {
  return periodType === 'FY';
}

/**
 * Check if period type is quarterly (Q1, Q2, Q3)
 * Note: Q4 is typically reported as FY
 */
export function isQuarterlyPeriod(periodType: string | null | undefined): boolean {
  if (!periodType) return false;
  return ['Q1', 'Q2', 'Q3'].includes(periodType);
}

/**
 * Check if EPS value is valid for valuation calculations
 * Excludes null, zero (forecast indicator), and non-finite values
 */
export function isValidEps(eps: number | null | undefined): eps is number {
  return typeof eps === 'number' && Number.isFinite(eps) && eps !== 0;
}

/**
 * Check if BPS value is valid for valuation calculations
 * Excludes null, zero, negative, and non-finite values
 */
export function isValidBps(bps: number | null | undefined): bps is number {
  return typeof bps === 'number' && Number.isFinite(bps) && bps > 0;
}

/**
 * Check if valuation metrics (EPS or BPS) are valid
 * Used to filter out forecast-only FY data
 */
export function hasValidValuationMetrics(eps: number | null | undefined, bps: number | null | undefined): boolean {
  return isValidEps(eps) || isValidBps(bps);
}

/**
 * Input type for hasActualFinancialData check
 * Minimal interface compatible with both API responses and internal data
 */
export interface FinancialDataInput {
  roe?: number | null;
  eps?: number | null;
  netProfit?: number | null;
  equity?: number | null;
}

/**
 * Check if data point has actual financial data (not just a forecast)
 * Forecasts typically have null/undefined financial data fields.
 *
 * Used to distinguish actual financial results from future projections
 * when selecting latestMetrics or filtering FY data.
 *
 * Note: Zero or negative values ARE valid actual data (losses, negative equity).
 * Only null/undefined indicates forecast/missing data.
 * EPS === 0 is special-cased because JQuants uses it as a forecast indicator.
 */
export function hasActualFinancialData(data: FinancialDataInput): boolean {
  // ROE present means actual data exists
  if (data.roe !== null && data.roe !== undefined) return true;

  // EPS === 0 indicates forecast in JQuants API, so use isValidEps
  if (isValidEps(data.eps)) return true;

  // Zero/negative netProfit and equity are valid actual values (losses, 債務超過)
  if (typeof data.netProfit === 'number') return true;
  if (typeof data.equity === 'number') return true;

  return false;
}

/**
 * FY data point for daily valuation calculation
 */
export interface FYDataPoint {
  disclosedDate: string;
  eps: number | null;
  bps: number | null;
}

/**
 * Filter FY data points with valid valuation metrics
 *
 * @param data Array of statement-like objects
 * @param getFields Function to extract relevant fields from each record
 * @returns Filtered and sorted FYDataPoints (sorted by disclosedDate ascending)
 */
export function filterValidFYData<T>(
  data: T[],
  getFields: (item: T) => { periodType: string; disclosedDate: string; eps: number | null; bps: number | null }
): FYDataPoint[] {
  return data
    .map(getFields)
    .filter((d) => isFiscalYear(d.periodType))
    .filter((d) => hasValidValuationMetrics(d.eps, d.bps))
    .map((d) => ({
      disclosedDate: d.disclosedDate,
      eps: d.eps,
      bps: d.bps,
    }))
    .sort((a, b) => a.disclosedDate.localeCompare(b.disclosedDate));
}

/**
 * Calculate PER and PBR from stock price and FY data
 *
 * @param close Stock closing price
 * @param fy FY data point with EPS and BPS
 * @returns PER and PBR values (null if calculation not possible)
 */
export function calculateDailyValuation(
  close: number,
  fy: FYDataPoint | null
): { per: number | null; pbr: number | null } {
  if (!fy) {
    return { per: null, pbr: null };
  }

  const per = isValidEps(fy.eps) ? Math.round((close / fy.eps) * 100) / 100 : null;
  const pbr = isValidBps(fy.bps) ? Math.round((close / fy.bps) * 100) / 100 : null;

  return { per, pbr };
}

/**
 * Find the most recent FY data point disclosed before or on a given date.
 * FY data points must be sorted by disclosure date ascending.
 *
 * @param fyDataPoints Sorted array of FY data points
 * @param dateStr Target date in YYYY-MM-DD format
 * @returns Most recent applicable FY, or null if none
 */
export function findApplicableFY(fyDataPoints: FYDataPoint[], dateStr: string): FYDataPoint | null {
  let applicableFY: FYDataPoint | null = null;
  for (const fy of fyDataPoints) {
    if (fy.disclosedDate <= dateStr) {
      applicableFY = fy;
    } else {
      break; // FY data points are sorted, so we can stop early
    }
  }
  return applicableFY;
}
