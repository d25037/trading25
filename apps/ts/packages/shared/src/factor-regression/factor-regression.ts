/**
 * Factor Analysis - Two-Stage Factor Regression
 *
 * Main factor regression implementation for risk decomposition
 */

import { getIndexCodesByCategory, getIndexDefinition } from '../db/constants/index-master-data';
import { INDEX_CATEGORIES, type IndexCategory } from '../db/schema/market-schema';
import { olsRegression } from './regression';
import type { AlignedReturns } from './returns';
import { alignReturns } from './returns';
import type { DailyReturn, FactorRegressionOptions, FactorRegressionResult, IndexMatch } from './types';
import { FactorRegressionError } from './types';

/**
 * Default options for factor regression
 */
const DEFAULT_OPTIONS: Required<FactorRegressionOptions> = {
  lookbackDays: 252,
  minDataPoints: 60,
};

/**
 * TOPIX code constant
 */
const TOPIX_CODE = '0000';

/**
 * Perform two-stage factor regression analysis
 *
 * Stage 1: Market regression
 *   r_stock = alpha + beta_m * r_TOPIX + residual
 *
 * Stage 2: Residual factor matching
 *   For each index category, regress residuals against each index
 *   and select top 3 by R-squared
 *
 * @param stockCode - Stock code for the analysis
 * @param stockReturns - Daily returns for the stock
 * @param topixReturns - Daily returns for TOPIX (code: 0000)
 * @param indicesReturns - Map of index code -> daily returns
 * @param options - Analysis options
 * @param companyName - Optional company name
 * @returns Factor regression result
 */
export function performFactorRegression(
  stockCode: string,
  stockReturns: DailyReturn[],
  topixReturns: DailyReturn[],
  indicesReturns: Map<string, DailyReturn[]>,
  options?: FactorRegressionOptions,
  companyName?: string
): FactorRegressionResult {
  const opts = { ...DEFAULT_OPTIONS, ...options };

  // Align stock returns with TOPIX
  const aligned = alignReturns(stockReturns, topixReturns);

  // Validate data points
  if (aligned.dates.length < opts.minDataPoints) {
    throw new FactorRegressionError(
      `Insufficient data points: ${aligned.dates.length} < ${opts.minDataPoints}`,
      'INSUFFICIENT_DATA',
      { dataPoints: aligned.dates.length, minRequired: opts.minDataPoints }
    );
  }

  // Limit to lookbackDays
  const limitedAligned = limitToLookback(aligned, opts.lookbackDays);

  // Stage 1: Market regression
  const marketRegression = olsRegression(limitedAligned.stockReturns, limitedAligned.indexReturns);

  // Stage 2: Residual factor matching
  const sector17Matches = findBestMatches(
    marketRegression.residuals,
    limitedAligned.dates,
    indicesReturns,
    getIndexCodesByCategory(INDEX_CATEGORIES.SECTOR17),
    3
  );

  const sector33Matches = findBestMatches(
    marketRegression.residuals,
    limitedAligned.dates,
    indicesReturns,
    getIndexCodesByCategory(INDEX_CATEGORIES.SECTOR33),
    3
  );

  // TOPIX size + MARKET + STYLE (excluding TOPIX 0000 which is used in Stage 1)
  const topixStyleCodes = [
    ...getIndexCodesByCategory(INDEX_CATEGORIES.TOPIX).filter((code) => code !== TOPIX_CODE),
    ...getIndexCodesByCategory(INDEX_CATEGORIES.MARKET),
    ...getIndexCodesByCategory(INDEX_CATEGORIES.STYLE),
  ];
  const topixStyleMatches = findBestMatches(
    marketRegression.residuals,
    limitedAligned.dates,
    indicesReturns,
    topixStyleCodes,
    3
  );

  // Get date range
  const sortedDates = [...limitedAligned.dates].sort();
  const firstDate = sortedDates[0] ?? '';
  const lastDate = sortedDates[sortedDates.length - 1] ?? '';

  return {
    stockCode,
    companyName,
    marketBeta: marketRegression.beta,
    marketRSquared: marketRegression.rSquared,
    sector17Matches,
    sector33Matches,
    topixStyleMatches,
    analysisDate: new Date().toISOString().split('T')[0] ?? '',
    dataPoints: limitedAligned.dates.length,
    dateRange: {
      from: firstDate,
      to: lastDate,
    },
  };
}

/**
 * Limit aligned returns to lookback period
 */
function limitToLookback(aligned: AlignedReturns, lookbackDays: number): AlignedReturns {
  if (aligned.dates.length <= lookbackDays) {
    return aligned;
  }

  const startIndex = aligned.dates.length - lookbackDays;
  return {
    dates: aligned.dates.slice(startIndex),
    stockReturns: aligned.stockReturns.slice(startIndex),
    indexReturns: aligned.indexReturns.slice(startIndex),
  };
}

/**
 * Build a map of date -> return from daily returns array
 */
function buildReturnMap(returns: DailyReturn[]): Map<string, number> {
  const map = new Map<string, number>();
  for (const item of returns) {
    map.set(item.date, item.return);
  }
  return map;
}

/**
 * Aligned residuals and index returns result
 */
interface AlignedData {
  residuals: number[];
  indexReturns: number[];
}

/**
 * Align residuals with index returns by date
 */
function alignResidualsWithIndex(
  residuals: number[],
  residualDates: string[],
  indexReturnMap: Map<string, number>
): AlignedData {
  const alignedResiduals: number[] = [];
  const alignedIndex: number[] = [];

  for (let i = 0; i < residualDates.length; i++) {
    const date = residualDates[i];
    const residual = residuals[i];
    if (date && residual !== undefined) {
      const indexReturn = indexReturnMap.get(date);
      if (indexReturn !== undefined) {
        alignedResiduals.push(residual);
        alignedIndex.push(indexReturn);
      }
    }
  }

  return { residuals: alignedResiduals, indexReturns: alignedIndex };
}

/**
 * Attempt to create an index match by regressing residuals against index returns
 * Returns undefined if regression fails or insufficient data
 */
function tryCreateIndexMatch(code: string, alignedData: AlignedData, minDataPoints: number): IndexMatch | undefined {
  if (alignedData.residuals.length < minDataPoints) {
    return undefined;
  }

  try {
    const regression = olsRegression(alignedData.residuals, alignedData.indexReturns);
    const indexDef = getIndexDefinition(code);

    return {
      indexCode: code,
      indexName: indexDef?.name ?? code,
      category: (indexDef?.category ?? 'unknown') as IndexCategory,
      rSquared: regression.rSquared,
      beta: regression.beta,
    };
  } catch {
    return undefined;
  }
}

/**
 * Find best matching indices from a category
 * Returns top N indices sorted by R-squared (descending)
 */
function findBestMatches(
  residuals: number[],
  residualDates: string[],
  indicesReturns: Map<string, DailyReturn[]>,
  categoryCodes: string[],
  topN: number
): IndexMatch[] {
  const matches: IndexMatch[] = [];
  const minDataPoints = 30;

  for (const code of categoryCodes) {
    const indexReturns = indicesReturns.get(code);
    if (!indexReturns || indexReturns.length === 0) {
      continue;
    }

    const indexReturnMap = buildReturnMap(indexReturns);
    const alignedData = alignResidualsWithIndex(residuals, residualDates, indexReturnMap);
    const match = tryCreateIndexMatch(code, alignedData, minDataPoints);

    if (match) {
      matches.push(match);
    }
  }

  // Sort by R-squared descending and return top N
  matches.sort((a, b) => b.rSquared - a.rSquared);
  return matches.slice(0, topN);
}
