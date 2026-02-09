/**
 * Dataset V2 - API Client Wrapper
 * Simplified wrapper around JQuantsClient with integrated rate limiting
 */

import type { JQuantsClient } from '../clients/JQuantsClient';
import type {
  JQuantsDailyQuote,
  JQuantsDailyQuotesResponse,
  JQuantsIndex,
  JQuantsIndicesResponse,
  JQuantsListedInfo,
  JQuantsListedInfoResponse,
  JQuantsStatement,
  JQuantsStatementsResponse,
  JQuantsTOPIX,
  JQuantsTOPIXResponse,
  JQuantsWeeklyMarginInterest,
  JQuantsWeeklyMarginInterestResponse,
} from '../types/jquants';
import type { DateRange, DebugConfig, MarginData, SectorData, StatementsData, StockInfo, TopixData } from './types';
import { ApiError, DEFAULT_DEBUG_CONFIG } from './types';

/**
 * Response structure analysis result
 */
interface ResponseStructureAnalysis {
  hasDataArray: boolean;
  dataType: string;
  hasPaginationKey: boolean;
  paginationKeyType: string;
  responseKeys: string[];
  responseSize: number;
}

/**
 * Sanitized statement data for logging
 */
interface SanitizedStatement {
  Code: unknown;
  DiscDate: unknown;
  CurPerType: unknown;
  DocType: unknown;
  hasEPS: boolean;
  epsType: string;
  allKeys: string[];
}

/**
 * Filtering details for validation reporting
 */
interface FilteringDetails {
  filtered: number;
  filterRate: number;
  retentionRate: number;
}

/**
 * API response structure for debug logging
 */
interface ApiResponse {
  data?: unknown[];
  pagination_key?: unknown;
  [key: string]: unknown;
}

import {
  safeValidateStockDataArray,
  validateMarginData,
  validateSectorData,
  validateStatementsData,
  validateStockInfoArray,
  validateTopixData,
} from './validators/runtime-validators';

/**
 * Convert numeric market code to string market type
 * Returns null for non-TSE main markets to exclude them from filtering
 */
function convertMarketCode(marketCode: string): string | null {
  const isDebugMode = process.env.DATASET_DEBUG === 'true';

  if (isDebugMode) {
    console.log(`[CONVERT MARKET] Input marketCode: "${marketCode}"`);
  }

  switch (marketCode) {
    case '0111':
      if (isDebugMode) console.log(`[CONVERT MARKET] ${marketCode} -> prime`);
      return 'prime';
    case '0112':
      if (isDebugMode) console.log(`[CONVERT MARKET] ${marketCode} -> standard`);
      return 'standard';
    case '0113':
      if (isDebugMode) console.log(`[CONVERT MARKET] ${marketCode} -> growth`);
      return 'growth';
    default:
      // Exclude non-TSE main markets (Prime/Standard/Growth)
      // This includes: 0105 (TOKYO PRO), 0106/0107 (JASDAQ), 0101/0102 (legacy), 0104 (Mothers), 0109 (other)
      if (isDebugMode) console.log(`[CONVERT MARKET] ${marketCode} -> null (excluded non-TSE main market)`);
      return null;
  }
}

/**
 * Parse and validate listed date from API response
 */
function parseListedDate(dateStr: string, stockCode: string): Date {
  const isDebugMode = process.env.DATASET_DEBUG === 'true';

  if (!dateStr || dateStr.trim() === '') {
    if (isDebugMode) {
      console.log(`DEBUG: Empty date for stock ${stockCode}, using default 1970-01-01`);
    }
    return new Date('1970-01-01');
  }

  const listedDate = new Date(dateStr);
  if (Number.isNaN(listedDate.getTime())) {
    if (isDebugMode) {
      console.log(`DEBUG: Invalid date "${dateStr}" for stock ${stockCode}, using default 1970-01-01`);
    }
    return new Date('1970-01-01');
  }

  return listedDate;
}

/**
 * Transform a single JQuants v2 stock info item to internal format
 * Returns null if the market is not a TSE main market
 */
function transformSingleStockInfo(item: JQuantsListedInfo, index: number): StockInfo | null {
  const isDebugMode = process.env.DATASET_DEBUG === 'true';
  const dateStr = item.Date as string;
  const listedDate = parseListedDate(dateStr, item.Code as string);

  const marketCode = convertMarketCode(item.Mkt as string);

  // Skip stocks that are not in TSE main markets (Prime, Standard, Growth)
  if (marketCode === null) {
    if (isDebugMode && index < 3) {
      console.log(`DEBUG: Skipping stock ${index + 1} (${item.Code}) - non-TSE main market: ${item.MktNm}`);
    }
    return null;
  }

  const stockInfo = {
    code: item.Code as string,
    companyName: item.CoName as string,
    companyNameEnglish: item.CoNameEn || '',
    marketCode,
    marketName: item.MktNm as string,
    sector17Code: item.S17 as string,
    sector17Name: item.S17Nm as string,
    sector33Code: item.S33 as string,
    sector33Name: item.S33Nm as string,
    scaleCategory: item.ScaleCat || '',
    listedDate,
  };

  if (isDebugMode && index < 3) {
    console.log(`DEBUG: Transformed stock ${index + 1}:`, {
      code: stockInfo.code,
      companyName: stockInfo.companyName,
      marketCode: stockInfo.marketCode,
      listedDate: stockInfo.listedDate.toISOString(),
      originalDate: dateStr,
    });
  }

  return stockInfo;
}

/**
 * Response transformation utilities with runtime type validation
 */
function transformStockInfo(apiResponse: JQuantsListedInfoResponse): StockInfo[] {
  const isDebugMode = process.env.DATASET_DEBUG === 'true';

  if (isDebugMode) {
    console.log(`DEBUG: transformStockInfo received ${apiResponse.data.length} stocks`);
    console.log(`DEBUG: First 3 raw API items:`, apiResponse.data.slice(0, 3));
  }

  const transformedStocks = apiResponse.data.map(transformSingleStockInfo);

  // Filter out null values (non-TSE main market stocks)
  const validStocks = transformedStocks.filter((stock): stock is StockInfo => stock !== null);

  if (isDebugMode) {
    const excludedCount = transformedStocks.length - validStocks.length;
    console.log(`DEBUG: Excluded ${excludedCount} non-TSE main market stocks`);
    console.log(`DEBUG: Remaining ${validStocks.length} TSE main market stocks`);
  }

  try {
    return validateStockInfoArray(validStocks);
  } catch (error) {
    throw new ApiError(
      `Stock info validation failed: ${error instanceof Error ? error.message : String(error)}`,
      'VALIDATION_ERROR',
      error instanceof Error ? error : new Error(String(error))
    );
  }
}

function transformDailyQuotes(apiResponse: JQuantsDailyQuotesResponse) {
  const transformedQuotes = apiResponse.data.map((quote: JQuantsDailyQuote) => ({
    code: quote.Code,
    date: new Date(quote.Date),
    open: quote.AdjO ?? quote.O ?? 0,
    high: quote.AdjH ?? quote.H ?? 0,
    low: quote.AdjL ?? quote.L ?? 0,
    close: quote.AdjC ?? quote.C ?? 0,
    volume: quote.AdjVo ?? quote.Vo ?? 0,
    adjustmentFactor: quote.AdjFactor,
  }));

  // Use safe validation to handle partial failures gracefully
  const { valid, invalid } = safeValidateStockDataArray(transformedQuotes);

  if (invalid.length > 0) {
    const isDebugMode = process.env.DATASET_DEBUG === 'true';
    if (isDebugMode) {
      console.warn(
        `WARNING: ${invalid.length} invalid stock data records filtered out:`,
        invalid.map((i) => ({ index: i.index, error: i.error }))
      );
    }
  }

  return valid;
}

/**
 * Validate array of data records, filtering out invalid entries
 * Generic helper to reduce duplication across transform functions
 */
function validateArrayWithLogging<T>(data: T[], validator: (item: T) => T, getItemLabel: (item: T) => string): T[] {
  const isDebugMode = process.env.DATASET_DEBUG === 'true';
  const validatedData: T[] = [];
  let invalidCount = 0;

  for (const item of data) {
    try {
      validatedData.push(validator(item));
    } catch (error) {
      invalidCount++;
      if (isDebugMode) {
        console.warn(
          `WARNING: Invalid data filtered out for ${getItemLabel(item)}:`,
          error instanceof Error ? error.message : String(error)
        );
      }
    }
  }

  if (invalidCount > 0 && isDebugMode) {
    console.warn(`Filtered out ${invalidCount} invalid data records`);
  }

  return validatedData;
}

function transformMarginData(apiResponse: JQuantsWeeklyMarginInterestResponse): MarginData[] {
  const transformedData = apiResponse.data.map((item: JQuantsWeeklyMarginInterest) => ({
    code: item.Code,
    date: new Date(item.Date),
    longMarginVolume: item.LongVol,
    shortMarginVolume: item.ShrtVol,
  }));

  return validateArrayWithLogging(transformedData, validateMarginData, (data) => data.code);
}

function transformTopixData(apiResponse: JQuantsTOPIXResponse): TopixData[] {
  const transformedData = apiResponse.data.map((item: JQuantsTOPIX) => ({
    date: new Date(item.Date),
    open: item.O,
    high: item.H,
    low: item.L,
    close: item.C,
    volume: 0, // TOPIX does not have volume data
  }));

  return validateArrayWithLogging(
    transformedData,
    validateTopixData,
    (data) => data.date.toISOString().split('T')[0] ?? 'unknown-date'
  );
}

function transformSectorData(apiResponse: JQuantsIndicesResponse): SectorData[] {
  const transformedData = apiResponse.data.map((item: JQuantsIndex) => ({
    sectorCode: item.Code,
    sectorName: '', // JQuantsIndex doesn't have name, would need to map from code
    date: new Date(item.Date),
    open: item.O ?? 0,
    high: item.H ?? 0,
    low: item.L ?? 0,
    close: item.C ?? 0,
    volume: 0, // JQuantsIndex doesn't have volume data
  }));

  return validateArrayWithLogging(transformedData, validateSectorData, (data) => data.sectorCode);
}

/**
 * Parse earnings per share with null safety (v2 field: EPS)
 */
function parseEarningsPerShare(item: JQuantsStatement): number | null {
  if (item.EPS === null || item.EPS === undefined) {
    return null;
  }
  const eps = typeof item.EPS === 'string' ? Number.parseFloat(item.EPS) : item.EPS;
  return Number.isNaN(eps) ? null : eps;
}

/**
 * Parse profit with consolidated/non-consolidated fallback (v2 field: NP, NCNP)
 */
function parseProfit(item: JQuantsStatement): number | null {
  if (item.NP !== null && item.NP !== undefined) {
    const profit = typeof item.NP === 'string' ? Number.parseFloat(item.NP) : item.NP;
    return Number.isNaN(profit) ? null : profit;
  }

  if (item.NCNP !== null && item.NCNP !== undefined) {
    const profit = typeof item.NCNP === 'string' ? Number.parseFloat(item.NCNP) : item.NCNP;
    return Number.isNaN(profit) ? null : profit;
  }

  return null;
}

/**
 * Parse equity with consolidated/non-consolidated fallback (v2 field: Eq, NCEq)
 */
function parseEquity(item: JQuantsStatement): number | null {
  if (item.Eq !== null && item.Eq !== undefined) {
    const equity = typeof item.Eq === 'string' ? Number.parseFloat(item.Eq) : item.Eq;
    return Number.isNaN(equity) ? null : equity;
  }

  if (item.NCEq !== null && item.NCEq !== undefined) {
    const equity = typeof item.NCEq === 'string' ? Number.parseFloat(item.NCEq) : item.NCEq;
    return Number.isNaN(equity) ? null : equity;
  }

  return null;
}

/**
 * Parse next year forecast EPS with null safety (v2 field: NxFEPS)
 */
function parseNextYearForecastEPS(item: JQuantsStatement): number | null {
  if (item.NxFEPS === null || item.NxFEPS === undefined) {
    return null;
  }
  const nextYearEps = typeof item.NxFEPS === 'string' ? Number.parseFloat(item.NxFEPS) : item.NxFEPS;
  return Number.isNaN(nextYearEps) ? null : nextYearEps;
}

/**
 * Parse BPS (Book Value Per Share) with consolidated/non-consolidated fallback (v2 field: BPS, NCBPS)
 */
function parseBPS(item: JQuantsStatement): number | null {
  if (item.BPS !== null && item.BPS !== undefined) {
    const bps = typeof item.BPS === 'string' ? Number.parseFloat(item.BPS) : item.BPS;
    return Number.isNaN(bps) ? null : bps;
  }
  if (item.NCBPS !== null && item.NCBPS !== undefined) {
    const bps = typeof item.NCBPS === 'string' ? Number.parseFloat(item.NCBPS) : item.NCBPS;
    return Number.isNaN(bps) ? null : bps;
  }
  return null;
}

/**
 * Parse Sales (Net Sales) with consolidated/non-consolidated fallback (v2 field: Sales, NCSales)
 */
function parseSales(item: JQuantsStatement): number | null {
  if (item.Sales !== null && item.Sales !== undefined) {
    const sales = typeof item.Sales === 'string' ? Number.parseFloat(item.Sales) : item.Sales;
    return Number.isNaN(sales) ? null : sales;
  }
  if (item.NCSales !== null && item.NCSales !== undefined) {
    const sales = typeof item.NCSales === 'string' ? Number.parseFloat(item.NCSales) : item.NCSales;
    return Number.isNaN(sales) ? null : sales;
  }
  return null;
}

/**
 * Parse Operating Profit with consolidated/non-consolidated fallback (v2 field: OP, NCOP)
 */
function parseOperatingProfit(item: JQuantsStatement): number | null {
  if (item.OP !== null && item.OP !== undefined) {
    const op = typeof item.OP === 'string' ? Number.parseFloat(item.OP) : item.OP;
    return Number.isNaN(op) ? null : op;
  }
  if (item.NCOP !== null && item.NCOP !== undefined) {
    const op = typeof item.NCOP === 'string' ? Number.parseFloat(item.NCOP) : item.NCOP;
    return Number.isNaN(op) ? null : op;
  }
  return null;
}

/**
 * Parse Ordinary Profit with consolidated/non-consolidated fallback (v2 field: OdP, NCOdP)
 */
function parseOrdinaryProfit(item: JQuantsStatement): number | null {
  if (item.OdP !== null && item.OdP !== undefined) {
    const odp = typeof item.OdP === 'string' ? Number.parseFloat(item.OdP) : item.OdP;
    return Number.isNaN(odp) ? null : odp;
  }
  if (item.NCOdP !== null && item.NCOdP !== undefined) {
    const odp = typeof item.NCOdP === 'string' ? Number.parseFloat(item.NCOdP) : item.NCOdP;
    return Number.isNaN(odp) ? null : odp;
  }
  return null;
}

/**
 * Parse Operating Cash Flow (v2 field: CFO)
 */
function parseOperatingCashFlow(item: JQuantsStatement): number | null {
  if (item.CFO === null || item.CFO === undefined) {
    return null;
  }
  const cfo = typeof item.CFO === 'string' ? Number.parseFloat(item.CFO) : item.CFO;
  return Number.isNaN(cfo) ? null : cfo;
}

/**
 * Parse Dividend Per Share Fiscal Year (v2 field: DivFY)
 */
function parseDividendFY(item: JQuantsStatement): number | null {
  if (item.DivFY === null || item.DivFY === undefined) {
    return null;
  }
  const divFY = typeof item.DivFY === 'string' ? Number.parseFloat(item.DivFY) : item.DivFY;
  return Number.isNaN(divFY) ? null : divFY;
}

/**
 * Parse Forecast EPS for current FY (v2 field: FEPS)
 */
function parseForecastEPS(item: JQuantsStatement): number | null {
  if (item.FEPS === null || item.FEPS === undefined) {
    return null;
  }
  const feps = typeof item.FEPS === 'string' ? Number.parseFloat(item.FEPS) : item.FEPS;
  return Number.isNaN(feps) ? null : feps;
}

/**
 * Parse Investing Cash Flow (v2 field: CFI)
 */
function parseInvestingCashFlow(item: JQuantsStatement): number | null {
  if (item.CFI === null || item.CFI === undefined) {
    return null;
  }
  const cfi = typeof item.CFI === 'string' ? Number.parseFloat(item.CFI) : item.CFI;
  return Number.isNaN(cfi) ? null : cfi;
}

/**
 * Parse Financing Cash Flow (v2 field: CFF)
 */
function parseFinancingCashFlow(item: JQuantsStatement): number | null {
  if (item.CFF === null || item.CFF === undefined) {
    return null;
  }
  const cff = typeof item.CFF === 'string' ? Number.parseFloat(item.CFF) : item.CFF;
  return Number.isNaN(cff) ? null : cff;
}

/**
 * Parse Cash and Cash Equivalents (v2 field: CashEq)
 */
function parseCashAndEquivalents(item: JQuantsStatement): number | null {
  if (item.CashEq === null || item.CashEq === undefined) {
    return null;
  }
  const cashEq = typeof item.CashEq === 'string' ? Number.parseFloat(item.CashEq) : item.CashEq;
  return Number.isNaN(cashEq) ? null : cashEq;
}

/**
 * Parse Total Assets with consolidated/non-consolidated fallback (v2 field: TA, NCTA)
 */
function parseTotalAssets(item: JQuantsStatement): number | null {
  if (item.TA !== null && item.TA !== undefined) {
    const ta = typeof item.TA === 'string' ? Number.parseFloat(item.TA) : item.TA;
    return Number.isNaN(ta) ? null : ta;
  }
  if (item.NCTA !== null && item.NCTA !== undefined) {
    const ta = typeof item.NCTA === 'string' ? Number.parseFloat(item.NCTA) : item.NCTA;
    return Number.isNaN(ta) ? null : ta;
  }
  return null;
}

/**
 * Parse Shares Outstanding at FY End (v2 field: ShOutFY)
 */
function parseSharesOutstanding(item: JQuantsStatement): number | null {
  if (item.ShOutFY === null || item.ShOutFY === undefined) {
    return null;
  }
  const shOut = typeof item.ShOutFY === 'string' ? Number.parseFloat(item.ShOutFY) : item.ShOutFY;
  return Number.isNaN(shOut) ? null : shOut;
}

/**
 * Parse Treasury Shares at FY End (v2 field: TrShFY)
 */
function parseTreasuryShares(item: JQuantsStatement): number | null {
  if (item.TrShFY === null || item.TrShFY === undefined) {
    return null;
  }
  const trSh = typeof item.TrShFY === 'string' ? Number.parseFloat(item.TrShFY) : item.TrShFY;
  return Number.isNaN(trSh) ? null : trSh;
}

/**
 * Transform a single statement with debug logging (v2 format)
 */
function transformSingleStatement(item: JQuantsStatement, index: number, isDebugMode: boolean): StatementsData {
  const earningsPerShare = parseEarningsPerShare(item);
  const profit = parseProfit(item);
  const equity = parseEquity(item);
  const nextYearForecastEarningsPerShare = parseNextYearForecastEPS(item);
  const typeOfCurrentPeriod = item.CurPerType ?? '';
  const typeOfDocument = item.DocType ?? '';
  // Extended financial metrics (added 2026-01)
  const bps = parseBPS(item);
  const sales = parseSales(item);
  const operatingProfit = parseOperatingProfit(item);
  const ordinaryProfit = parseOrdinaryProfit(item);
  const operatingCashFlow = parseOperatingCashFlow(item);
  const dividendFY = parseDividendFY(item);
  const forecastEps = parseForecastEPS(item);
  // Cash flow extended metrics (added 2026-01)
  const investingCashFlow = parseInvestingCashFlow(item);
  const financingCashFlow = parseFinancingCashFlow(item);
  const cashAndEquivalents = parseCashAndEquivalents(item);
  const totalAssets = parseTotalAssets(item);
  const sharesOutstanding = parseSharesOutstanding(item);
  const treasuryShares = parseTreasuryShares(item);

  const transformed = {
    code: item.Code as string,
    disclosedDate: new Date(item.DiscDate as string),
    earningsPerShare,
    profit,
    equity,
    typeOfCurrentPeriod,
    typeOfDocument,
    nextYearForecastEarningsPerShare,
    // Extended financial metrics
    bps,
    sales,
    operatingProfit,
    ordinaryProfit,
    operatingCashFlow,
    dividendFY,
    forecastEps,
    // Cash flow extended metrics
    investingCashFlow,
    financingCashFlow,
    cashAndEquivalents,
    totalAssets,
    sharesOutstanding,
    treasuryShares,
  };

  if (isDebugMode && index === 0) {
    console.log(`[API CLIENT] 📊 Enhanced financial data transformation for ${item.Code}:`, {
      earningsPerShare: { original: item.EPS, parsed: earningsPerShare },
      profit: {
        consolidated: item.NP,
        nonConsolidated: item.NCNP,
        selected: profit,
      },
      equity: {
        consolidated: item.Eq,
        nonConsolidated: item.NCEq,
        selected: equity,
      },
      nextYearForecast: { original: item.NxFEPS, parsed: nextYearForecastEarningsPerShare },
      bps: { consolidated: item.BPS, nonConsolidated: item.NCBPS, selected: bps },
      sales: { consolidated: item.Sales, nonConsolidated: item.NCSales, selected: sales },
      operatingProfit: { consolidated: item.OP, nonConsolidated: item.NCOP, selected: operatingProfit },
      ordinaryProfit: { consolidated: item.OdP, nonConsolidated: item.NCOdP, selected: ordinaryProfit },
      operatingCashFlow: { original: item.CFO, parsed: operatingCashFlow },
      dividendFY: { original: item.DivFY, parsed: dividendFY },
      forecastEps: { original: item.FEPS, parsed: forecastEps },
    });
  }

  return transformed;
}

function validateStatementsArray(transformedData: StatementsData[], isDebugMode: boolean): StatementsData[] {
  // Use generic validator with additional debug logging for statements
  const validatedData = validateArrayWithLogging(
    transformedData,
    validateStatementsData,
    (data) => `${data.code} on ${data.disclosedDate.toISOString().split('T')[0] ?? 'unknown-date'}`
  );

  if (isDebugMode) {
    console.log(
      `DEBUG: Statements validation complete: ${validatedData.length} valid, ${transformedData.length - validatedData.length} invalid`
    );
  }

  return validatedData;
}

function transformStatementsData(
  apiResponse: JQuantsStatementsResponse,
  debugConfig?: { enabled: boolean }
): StatementsData[] {
  const isDebugMode = debugConfig?.enabled ?? false;

  if (isDebugMode) {
    console.log(`[API CLIENT] Statements API response received with ${apiResponse.data?.length || 0} items`);
  }

  if (!apiResponse.data || apiResponse.data.length === 0) {
    if (isDebugMode) {
      console.log('[API CLIENT] Empty data array in API response');
    }
    return [];
  }

  if (isDebugMode) {
    console.log(`[API CLIENT] First statement sample:`, apiResponse.data[0]);
  }

  if (isDebugMode) {
    console.log(`[API CLIENT] Transforming ${apiResponse.data.length} statements...`);
    console.log(`[API CLIENT] Sample raw statement:`, apiResponse.data[0]);
  }

  const transformedData = apiResponse.data.map((item: JQuantsStatement, index: number) =>
    transformSingleStatement(item, index, isDebugMode)
  );

  if (isDebugMode) {
    console.log(`[API CLIENT] Transformed ${transformedData.length} statements records`);
  }

  return validateStatementsArray(transformedData, isDebugMode);
}

/**
 * Simplified API client wrapper
 * Note: Retry logic is handled by RateLimiter, not here
 */
export class ApiClient {
  constructor(
    public client: JQuantsClient,
    private debugConfig: DebugConfig = DEFAULT_DEBUG_CONFIG
  ) {}

  /**
   * Fetch stock list
   */
  async getStockList(): Promise<StockInfo[]> {
    const response = await this.client.getListedInfo();
    return transformStockInfo(response);
  }

  /**
   * Fetch daily quotes for a stock
   */
  async getStockQuotes(stockCode: string, dateRange?: DateRange) {
    const response = await this.client.getDailyQuotes({
      code: stockCode,
      from: dateRange?.from?.toISOString().split('T')[0],
      to: dateRange?.to?.toISOString().split('T')[0],
    });
    return transformDailyQuotes(response);
  }

  /**
   * Fetch margin data for a stock
   */
  async getMarginData(stockCode: string, dateRange?: DateRange): Promise<MarginData[]> {
    const response = await this.client.getWeeklyMarginInterest({
      code: stockCode,
      from: dateRange?.from?.toISOString().split('T')[0],
      to: dateRange?.to?.toISOString().split('T')[0],
    });
    return transformMarginData(response);
  }

  /**
   * Fetch TOPIX index data
   */
  async getTopixData(dateRange?: DateRange): Promise<TopixData[]> {
    const params = {
      from: dateRange?.from?.toISOString().split('T')[0],
      to: dateRange?.to?.toISOString().split('T')[0],
    };
    console.log(`[API CLIENT] getTOPIX params:`, params);

    const response = await this.client.getTOPIX(params);
    console.log(`[API CLIENT] getTOPIX response: ${response.data?.length ?? 0} records`);

    return transformTopixData(response);
  }

  /**
   * Fetch sector indices data.
   * Pagination is handled automatically by JQuantsClient.
   */
  async getSectorIndices(sectorCode?: string, dateRange?: DateRange): Promise<SectorData[]> {
    const fromStr = dateRange?.from?.toISOString().split('T')[0];
    const toStr = dateRange?.to?.toISOString().split('T')[0];

    const response = await this.client.getIndices({
      code: sectorCode,
      from: fromStr,
      to: toStr,
    });
    return transformSectorData(response);
  }

  /**
   * Fetch financial statements data with enhanced debugging
   */
  async getStatementsData(stockCode: string, _dateRange?: DateRange): Promise<StatementsData[]> {
    const isDebugMode = this.debugConfig.enabled;
    const startTime = Date.now();

    if (isDebugMode) {
      console.log(`[API CLIENT] ==========================================`);
      console.log(`[API CLIENT] ENTRY: Fetching statements for ${stockCode}...`);
      console.log(`[API CLIENT] Request params: { code: "${stockCode}" }`);
      console.log(`[API CLIENT] Date range ignored: ${_dateRange ? 'yes (dateRange provided but ignored)' : 'no'}`);
      console.log(`[API CLIENT] ==========================================`);
    }

    return this.executeStatementsRequest(stockCode, startTime, isDebugMode);
  }

  /**
   * Execute the actual statements API request with comprehensive logging
   */
  private async executeStatementsRequest(
    stockCode: string,
    startTime: number,
    isDebugMode: boolean
  ): Promise<StatementsData[]> {
    const apiCallStart = Date.now();

    try {
      const response = await this.client.getStatements({
        code: stockCode,
      });

      const apiCallTime = Date.now() - apiCallStart;
      this.logApiResponse(stockCode, response as ApiResponse, apiCallTime, isDebugMode);

      const transformStart = Date.now();
      const result = transformStatementsData(response, this.debugConfig);
      const transformTime = Date.now() - transformStart;
      const totalTime = Date.now() - startTime;

      this.logProcessingComplete(
        stockCode,
        apiCallTime,
        transformTime,
        totalTime,
        response as ApiResponse,
        result,
        isDebugMode
      );

      return result;
    } catch (error) {
      const errorTime = Date.now() - apiCallStart;
      this.logApiError(stockCode, error, errorTime, isDebugMode);
      throw error;
    }
  }

  /**
   * Log API response details
   */
  private logApiResponse(stockCode: string, response: ApiResponse, apiCallTime: number, isDebugMode: boolean): void {
    if (!isDebugMode) return;

    console.log(`[API CLIENT] Raw API response for ${stockCode} (${apiCallTime}ms):`, {
      hasData: !!response.data,
      dataCount: response.data?.length || 0,
      paginationKey: response.pagination_key,
      responseStructure: this.analyzeResponseStructure(response),
    });

    // Log sample statement data if available
    if (response.data && response.data.length > 0) {
      console.log(`[API CLIENT] Sample statement for ${stockCode}:`, {
        firstStatement: this.sanitizeStatementForLogging(response.data[0] as Record<string, unknown>),
        lastStatement:
          response.data.length > 1
            ? this.sanitizeStatementForLogging(response.data[response.data.length - 1] as Record<string, unknown>)
            : null,
      });
    }
  }

  /**
   * Log processing completion details
   */
  private logProcessingComplete(
    stockCode: string,
    apiCallTime: number,
    transformTime: number,
    totalTime: number,
    response: ApiResponse,
    result: StatementsData[],
    isDebugMode: boolean
  ): void {
    if (!isDebugMode) return;

    console.log(`[API CLIENT] Processing complete for ${stockCode}:`, {
      apiCallTime: `${apiCallTime}ms`,
      transformTime: `${transformTime}ms`,
      totalTime: `${totalTime}ms`,
      rawCount: response.data?.length || 0,
      validatedCount: result.length,
      filteringDetails: this.getFilteringDetails(response.data?.length || 0, result.length),
    });
  }

  /**
   * Log API error details
   */
  private logApiError(stockCode: string, error: unknown, errorTime: number, isDebugMode: boolean): void {
    if (!isDebugMode) return;

    console.log(`[API CLIENT] Error fetching statements for ${stockCode} (${errorTime}ms):`, {
      errorType: typeof error,
      errorMessage: error instanceof Error ? error.message : String(error),
      errorStack: error instanceof Error ? error.stack?.split('\n').slice(0, 3) : undefined,
    });
  }

  /**
   * Analyze response structure for debugging
   */
  private analyzeResponseStructure(response: ApiResponse): ResponseStructureAnalysis {
    return {
      hasDataArray: Array.isArray(response.data),
      dataType: typeof response.data,
      hasPaginationKey: !!response.pagination_key,
      paginationKeyType: typeof response.pagination_key,
      responseKeys: Object.keys(response),
      responseSize: JSON.stringify(response).length,
    };
  }

  /**
   * Sanitize statement data for safe logging (v2 format)
   */
  private sanitizeStatementForLogging(statement: Record<string, unknown>): SanitizedStatement {
    return {
      Code: statement.Code,
      DiscDate: statement.DiscDate,
      CurPerType: statement.CurPerType,
      DocType: statement.DocType,
      hasEPS: typeof statement.EPS !== 'undefined',
      epsType: typeof statement.EPS,
      allKeys: Object.keys(statement),
    };
  }

  /**
   * Get filtering details for validation
   */
  private getFilteringDetails(rawCount: number, validatedCount: number): FilteringDetails {
    const filtered = rawCount - validatedCount;
    return {
      filtered: filtered,
      filterRate: rawCount > 0 ? Math.round((filtered / rawCount) * 100) : 0,
      retentionRate: rawCount > 0 ? Math.round((validatedCount / rawCount) * 100) : 0,
    };
  }
}
