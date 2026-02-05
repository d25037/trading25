/**
 * @deprecated This service is deprecated. Use apps/bt/ API via BacktestClient.computeFundamentals() instead.
 * Maintained for fallback during deprecation period only.
 *
 * Migration: apps/ts/api/routes/analytics/fundamentals.ts now proxies to apps/bt/ API.
 * This file will be removed in a future release.
 */
import {
  calculateBPS,
  calculateDailyValuation,
  calculateDilutedEPS,
  calculateEPS,
  calculateFCFMargin,
  calculateFCFYield,
  calculateNetMargin,
  calculateOperatingMargin,
  calculatePBR,
  calculatePER,
  calculateROA,
  calculateROE,
  calculateSimpleFCF,
  type FYDataPoint,
  findApplicableFY,
  getAccountingStandard,
  getEquity,
  getNetProfit,
  getNetSales,
  getOperatingProfit,
  getSharesOutstanding,
  getTotalAssets,
  getTreasuryShares,
  hasActualFinancialData,
  hasValidValuationMetrics,
  isConsolidatedStatement,
  isFiscalYear,
  normalizePeriodType,
  toNumberOrNull,
} from '@trading25/shared/fundamental-analysis';
import { MarketDataReader } from '@trading25/shared/market-sync';
import type { JQuantsStatement } from '@trading25/shared/types/jquants';
import { getMarketDbPath } from '@trading25/shared/utils/dataset-paths';
import { logger } from '@trading25/shared/utils/logger';
import type { DailyValuationDataPoint, FundamentalDataPoint, FundamentalsResponse } from '../schemas/fundamentals';
import { BaseJQuantsService } from './base-jquants-service';

interface FundamentalsQueryOptions {
  symbol: string;
  from?: string;
  to?: string;
  periodType: 'all' | 'FY' | '1Q' | '2Q' | '3Q';
  preferConsolidated: boolean;
}

export class FundamentalsDataService extends BaseJQuantsService {
  private marketReader: MarketDataReader | null = null;

  /**
   * Get or create MarketDataReader instance
   */
  private getMarketReader(): MarketDataReader {
    if (!this.marketReader) {
      const dbPath = getMarketDbPath();
      this.marketReader = new MarketDataReader(dbPath);
    }
    return this.marketReader;
  }

  /**
   * Get fundamental metrics for a stock
   */
  async getFundamentals(options: FundamentalsQueryOptions): Promise<FundamentalsResponse> {
    logger.debug('Fetching fundamentals', { options });

    // Fetch financial statements from JQuants API
    const client = this.getJQuantsClient();
    const response = await this.withTokenRefresh(() => client.getStatements({ code: options.symbol }));

    if (!response.data || response.data.length === 0) {
      logger.debug('No financial statements found', { symbol: options.symbol });
      return {
        symbol: options.symbol,
        data: [],
        lastUpdated: new Date().toISOString(),
      };
    }

    logger.debug('Found financial statements', { count: response.data.length });

    // Calculate daily valuation time-series (uses all statements for FY lookup)
    const dailyValuation = this.calculateDailyValuation(options.symbol, response.data, options.preferConsolidated);

    // Filter statements by criteria
    const filteredStatements = this.filterStatements(response.data, options);

    if (filteredStatements.length === 0) {
      logger.debug('No statements match filter criteria');
      return {
        symbol: options.symbol,
        data: [],
        dailyValuation: dailyValuation.length > 0 ? dailyValuation : undefined,
        lastUpdated: new Date().toISOString(),
      };
    }

    // Get stock prices for disclosure dates
    const priceMap = this.getStockPricesForStatements(options.symbol, filteredStatements);

    // Calculate metrics for each statement
    const data = filteredStatements
      .map((statement) => this.calculateAllMetrics(statement, priceMap, options.preferConsolidated))
      .sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());

    // Get company name from first statement
    const companyName = await this.getCompanyName(options.symbol);

    // Get latest metrics - find first data point with actual financial data (not just forecast)
    // Forecasts have date in the future and empty financial data
    const latestMetrics = this.updateLatestMetricsWithDailyValuation(
      data.find((d) => hasActualFinancialData(d)),
      dailyValuation,
      data
    );

    // Update latestMetrics with forecast EPS and previous period CF data
    const enhancedLatestMetrics = this.enhanceLatestMetrics(latestMetrics, response.data, options.preferConsolidated);

    // Annotate latest FY with revised forecast from latest Q (if different)
    this.annotateLatestFYWithRevision(data, enhancedLatestMetrics, response.data, options.preferConsolidated);

    logger.debug('Fundamentals calculation complete', {
      symbol: options.symbol,
      dataPoints: data.length,
      dailyValuationPoints: dailyValuation.length,
    });

    return {
      symbol: options.symbol,
      companyName,
      data,
      latestMetrics: enhancedLatestMetrics,
      dailyValuation: dailyValuation.length > 0 ? dailyValuation : undefined,
      lastUpdated: new Date().toISOString(),
    };
  }

  /**
   * Filter statements by date range and period type
   */
  private filterStatements(statements: JQuantsStatement[], options: FundamentalsQueryOptions): JQuantsStatement[] {
    return statements.filter((stmt) => {
      // Filter by period type
      const normalizedPeriodType = normalizePeriodType(options.periodType);
      const stmtPeriodType = normalizePeriodType(stmt.CurPerType);
      if (normalizedPeriodType !== 'all' && stmtPeriodType !== normalizedPeriodType) {
        return false;
      }

      // Filter by date range
      if (options.from) {
        const fromDate = new Date(options.from);
        const periodEnd = new Date(stmt.CurPerEn);
        if (periodEnd < fromDate) {
          return false;
        }
      }

      if (options.to) {
        const toDate = new Date(options.to);
        const periodEnd = new Date(stmt.CurPerEn);
        if (periodEnd > toDate) {
          return false;
        }
      }

      return true;
    });
  }

  /**
   * Get stock prices for statement disclosure dates
   */
  private getStockPricesForStatements(symbol: string, statements: JQuantsStatement[]): Map<string, number> {
    try {
      const reader = this.getMarketReader();
      // Convert 4-digit symbol to 5-digit format for market.db lookup
      // market.db stores codes with trailing "0" (e.g., "70110" instead of "7011")
      const dbSymbol = `${symbol}0`;
      // Parse dates as UTC to match toISODateString() behavior in getPricesAtDates
      const dates = statements.map((stmt) => {
        const parts = stmt.DiscDate.split('-');
        const year = Number(parts[0]);
        const month = Number(parts[1]);
        const day = Number(parts[2]);
        return new Date(Date.UTC(year, month - 1, day));
      });
      return reader.getPricesAtDates(dbSymbol, dates);
    } catch (error) {
      logger.warn('Failed to get stock prices from market database', {
        symbol,
        error: error instanceof Error ? error.message : String(error),
      });
      return new Map();
    }
  }

  /**
   * Calculate all fundamental metrics for a statement
   */
  private calculateAllMetrics(
    statement: JQuantsStatement,
    priceMap: Map<string, number>,
    preferConsolidated: boolean
  ): FundamentalDataPoint {
    // Get stock price at disclosure date
    const stockPrice = priceMap.get(statement.DiscDate) || null;

    // Calculate core metrics
    const eps = calculateEPS(statement, preferConsolidated);
    const dilutedEps = calculateDilutedEPS(statement);
    const bps = calculateBPS(statement, preferConsolidated);

    // Calculate ROE
    let roe: number | null = null;
    try {
      const roeResult = calculateROE(statement, { preferConsolidated, annualize: true });
      roe = Math.round(roeResult.roe * 100) / 100;
    } catch {
      // ROE calculation failed, leave as null
    }

    // Calculate valuation metrics (PER, PBR)
    const per = calculatePER(eps, stockPrice);
    const pbr = calculatePBR(bps, stockPrice);

    // Calculate profitability metrics
    const roa = calculateROA(statement, preferConsolidated);
    const operatingMargin = calculateOperatingMargin(statement, preferConsolidated);
    const netMargin = calculateNetMargin(statement, preferConsolidated);

    // Get raw financial data
    const netProfit = getNetProfit(statement, preferConsolidated);
    const equity = getEquity(statement, preferConsolidated);
    const totalAssets = getTotalAssets(statement, preferConsolidated);
    const netSales = getNetSales(statement, preferConsolidated);
    const operatingProfit = getOperatingProfit(statement, preferConsolidated);

    const normalizedPeriodType = normalizePeriodType(statement.CurPerType);

    return {
      date: statement.CurPerEn,
      disclosedDate: statement.DiscDate,
      periodType: normalizedPeriodType ?? statement.CurPerType,
      isConsolidated: isConsolidatedStatement(statement),
      accountingStandard: getAccountingStandard(statement),
      // Core metrics
      roe,
      eps: eps !== null ? Math.round(eps * 100) / 100 : null,
      dilutedEps: dilutedEps !== null ? Math.round(dilutedEps * 100) / 100 : null,
      bps: bps !== null ? Math.round(bps * 100) / 100 : null,
      adjustedEps: null,
      adjustedForecastEps: null,
      adjustedBps: null,
      per: per !== null ? Math.round(per * 100) / 100 : null,
      pbr: pbr !== null ? Math.round(pbr * 100) / 100 : null,
      // Profitability metrics
      roa: roa !== null ? Math.round(roa * 100) / 100 : null,
      operatingMargin: operatingMargin !== null ? Math.round(operatingMargin * 100) / 100 : null,
      netMargin: netMargin !== null ? Math.round(netMargin * 100) / 100 : null,
      // Financial data in millions of JPY (stockPrice is per-share, not converted)
      stockPrice,
      netProfit: this.toMillions(toNumberOrNull(netProfit)),
      equity: this.toMillions(toNumberOrNull(equity)),
      totalAssets: this.toMillions(toNumberOrNull(totalAssets)),
      netSales: this.toMillions(toNumberOrNull(netSales)),
      operatingProfit: this.toMillions(toNumberOrNull(operatingProfit)),
      // Cash flow data in millions of JPY (consolidated only)
      cashFlowOperating: this.toMillions(toNumberOrNull(statement.CFO)),
      cashFlowInvesting: this.toMillions(toNumberOrNull(statement.CFI)),
      cashFlowFinancing: this.toMillions(toNumberOrNull(statement.CFF)),
      cashAndEquivalents: this.toMillions(toNumberOrNull(statement.CashEq)),
      // FCF metrics
      ...this.calculateFCFMetrics(statement, stockPrice, netSales),
      // Forecast EPS for this statement
      ...this.getForecastEps(statement, eps !== null ? Math.round(eps * 100) / 100 : null, preferConsolidated),
      revisedForecastEps: null,
      revisedForecastSource: null,
      prevCashFlowOperating: null,
      prevCashFlowInvesting: null,
      prevCashFlowFinancing: null,
      prevCashAndEquivalents: null,
    };
  }

  /**
   * Update latestMetrics with daily valuation data (PER/PBR from latest trading day)
   */
  private updateLatestMetricsWithDailyValuation(
    metrics: FundamentalDataPoint | undefined,
    dailyValuation: DailyValuationDataPoint[],
    data: FundamentalDataPoint[]
  ): FundamentalDataPoint | undefined {
    if (!metrics) return undefined;

    const latestDaily = dailyValuation[dailyValuation.length - 1];
    if (!latestDaily) {
      // Fallback: use FY data with disclosure date price
      return this.applyFYDataToMetrics(metrics, data);
    }

    const latestFY = data.find((d) => isFiscalYear(d.periodType) && hasActualFinancialData(d));

    logger.debug('latestMetrics updated with daily valuation', {
      date: latestDaily.date,
      close: latestDaily.close,
      per: latestDaily.per,
      pbr: latestDaily.pbr,
    });

    return {
      ...metrics,
      per: latestDaily.per,
      pbr: latestDaily.pbr,
      stockPrice: latestDaily.close,
      eps: latestFY?.eps ?? metrics.eps,
      bps: latestFY?.bps ?? metrics.bps,
    };
  }

  /**
   * Apply FY EPS/BPS to metrics for PER/PBR calculation (fallback)
   */
  private applyFYDataToMetrics(metrics: FundamentalDataPoint, data: FundamentalDataPoint[]): FundamentalDataPoint {
    const latestFY = data.find((d) => isFiscalYear(d.periodType) && hasActualFinancialData(d));

    if (!latestFY || latestFY.eps === null || latestFY.eps === 0) {
      return metrics;
    }

    const fyPer = metrics.stockPrice !== null ? Math.round((metrics.stockPrice / latestFY.eps) * 100) / 100 : null;
    const fyPbr =
      metrics.stockPrice !== null && latestFY.bps !== null && latestFY.bps > 0
        ? Math.round((metrics.stockPrice / latestFY.bps) * 100) / 100
        : null;

    logger.debug('PER/PBR recalculated using FY data (fallback)', {
      fyDate: latestFY.date,
      fyEps: latestFY.eps,
      fyBps: latestFY.bps,
      stockPrice: metrics.stockPrice,
      per: fyPer,
      pbr: fyPbr,
    });

    return {
      ...metrics,
      per: fyPer,
      pbr: fyPbr,
      eps: latestFY.eps,
      bps: latestFY.bps,
    };
  }

  /**
   * Get company name from market database
   * Converts 4-digit symbol to 5-digit format for database lookup
   */
  private async getCompanyName(symbol: string): Promise<string | undefined> {
    try {
      const reader = this.getMarketReader();
      // market.db stores codes with trailing "0" (e.g., "72030" instead of "7203")
      const dbSymbol = `${symbol}0`;
      const stockInfo = reader.getStockByCode(dbSymbol);
      return stockInfo?.companyName;
    } catch (error) {
      logger.warn('Failed to get company name', {
        symbol,
        error: error instanceof Error ? error.message : String(error),
      });
      return undefined;
    }
  }

  /**
   * Get all daily stock prices for time-series calculation
   * Returns prices for all available data in market.db (approximately 1 year)
   */
  private getDailyStockPrices(symbol: string): Map<string, number> {
    try {
      const reader = this.getMarketReader();
      // Convert 4-digit symbol to 5-digit format for market.db lookup
      const dbSymbol = `${symbol}0`;

      // Get all stock data
      const stockDataList = reader.getStockData(dbSymbol);

      const priceMap = new Map<string, number>();
      for (const data of stockDataList) {
        // Convert Date to ISO date string (YYYY-MM-DD)
        const dateStr = data.date.toISOString().split('T')[0];
        if (dateStr) {
          priceMap.set(dateStr, data.close);
        }
      }

      logger.debug('Daily stock prices loaded', {
        symbol,
        dataPoints: priceMap.size,
      });

      return priceMap;
    } catch (error) {
      logger.warn('Failed to get daily stock prices', {
        symbol,
        error: error instanceof Error ? error.message : String(error),
      });
      return new Map();
    }
  }

  /**
   * Get FY data points sorted by disclosure date for daily valuation calculation
   * For each trading day, we use the most recent FY data disclosed before that day
   * Excludes forecast data (eps=0/empty, bps=0/empty)
   */
  private getApplicableFYData(statements: JQuantsStatement[], preferConsolidated: boolean): FYDataPoint[] {
    return (
      statements
        .filter((stmt) => isFiscalYear(stmt.CurPerType))
        .map((stmt) => ({
          disclosedDate: stmt.DiscDate,
          // JQuants returns numbers as strings, so convert to numbers
          eps: toNumberOrNull(calculateEPS(stmt, preferConsolidated)),
          bps: toNumberOrNull(calculateBPS(stmt, preferConsolidated)),
        }))
        // Only include FY with actual financial data (exclude forecasts)
        .filter((fy) => hasValidValuationMetrics(fy.eps, fy.bps))
        .sort((a, b) => a.disclosedDate.localeCompare(b.disclosedDate))
    );
  }

  /**
   * Calculate daily PER/PBR time-series
   * Uses daily close prices with the most recent FY EPS/BPS as of each trading day
   */
  private calculateDailyValuation(
    symbol: string,
    statements: JQuantsStatement[],
    preferConsolidated: boolean
  ): DailyValuationDataPoint[] {
    const dailyPrices = this.getDailyStockPrices(symbol);
    if (dailyPrices.size === 0) {
      logger.debug('No daily prices available for daily valuation', { symbol });
      return [];
    }

    const fyDataPoints = this.getApplicableFYData(statements, preferConsolidated);
    if (fyDataPoints.length === 0) {
      logger.debug('No FY data available for daily valuation', { symbol });
      return [];
    }

    const tradingDates = Array.from(dailyPrices.keys()).sort();
    const result: DailyValuationDataPoint[] = [];

    for (const dateStr of tradingDates) {
      const close = dailyPrices.get(dateStr);
      if (close === undefined) continue;

      const applicableFY = findApplicableFY(fyDataPoints, dateStr);
      const { per, pbr } = calculateDailyValuation(close, applicableFY);

      result.push({ date: dateStr, close, per, pbr, marketCap: null });
    }

    logger.debug('Daily valuation calculated', {
      symbol,
      dataPoints: result.length,
      fyDataPointsUsed: fyDataPoints.length,
    });

    return result;
  }

  /**
   * Calculate FCF and derived metrics (yield, margin).
   * All ratio calculations use JPY internally; only FCF is converted to millions.
   */
  private calculateFCFMetrics(
    statement: JQuantsStatement,
    stockPrice: number | null,
    netSales: number | null
  ): { fcf: number | null; fcfYield: number | null; fcfMargin: number | null } {
    const cfo = toNumberOrNull(statement.CFO);
    const cfi = toNumberOrNull(statement.CFI);
    const fcf = calculateSimpleFCF(cfo, cfi);

    const sharesOutstanding = getSharesOutstanding(statement);
    const treasuryShares = getTreasuryShares(statement);
    const fcfYield = calculateFCFYield(fcf, stockPrice, sharesOutstanding, treasuryShares);
    const fcfMargin = calculateFCFMargin(fcf, toNumberOrNull(netSales));

    return {
      fcf: this.toMillions(this.roundOrNull(fcf)),
      fcfYield: this.roundOrNull(fcfYield),
      fcfMargin: this.roundOrNull(fcfMargin),
    };
  }

  /**
   * Enhance latest metrics with forecast EPS and previous period cash flow data
   */
  private enhanceLatestMetrics(
    metrics: FundamentalDataPoint | undefined,
    statements: JQuantsStatement[],
    preferConsolidated: boolean
  ): FundamentalDataPoint | undefined {
    if (!metrics) return undefined;

    // Sort statements by period end date descending
    const sortedByPeriodEnd = [...statements].sort((a, b) => b.CurPerEn.localeCompare(a.CurPerEn));

    // Find the statement corresponding to latestMetrics
    const currentStatement = sortedByPeriodEnd.find((s) => s.CurPerEn === metrics.date);

    // Get forecast EPS from the current statement
    const { forecastEps, forecastEpsChangeRate } = this.getForecastEps(
      currentStatement,
      metrics.eps,
      preferConsolidated
    );

    // Find previous period with cash flow data (same period type, one year earlier)
    const prevPeriodCF = this.getPreviousPeriodCashFlow(metrics.date, metrics.periodType, sortedByPeriodEnd);

    logger.debug('Enhanced latest metrics', {
      forecastEps,
      forecastEpsChangeRate,
      prevCashFlowOperating: prevPeriodCF.prevCashFlowOperating,
    });

    return {
      ...metrics,
      forecastEps,
      forecastEpsChangeRate,
      ...prevPeriodCF,
    };
  }

  /**
   * Annotate the latest FY data point with revised forecast from the latest Q statement.
   * Only sets revisedForecastEps when the latest Q's forecast differs from the FY's own NxFEPS.
   */
  private annotateLatestFYWithRevision(
    data: FundamentalDataPoint[],
    enhancedLatestMetrics: FundamentalDataPoint | undefined,
    statements: JQuantsStatement[],
    preferConsolidated: boolean
  ): void {
    if (!enhancedLatestMetrics) return;

    const latestFyIdx = data.findIndex((d) => isFiscalYear(d.periodType) && hasActualFinancialData(d));
    if (latestFyIdx === -1 || !data[latestFyIdx]) return;

    const latestFY = data[latestFyIdx];

    // Find the latest Q statement that is newer than the latest FY
    const sortedByDisc = [...statements]
      .filter((s) => !isFiscalYear(s.CurPerType))
      .sort((a, b) => b.DiscDate.localeCompare(a.DiscDate));
    const latestQ = sortedByDisc[0];
    if (!latestQ) return;

    // Ensure Q is disclosed after the latest FY
    if (latestQ.DiscDate <= latestFY.disclosedDate) return;

    // Get Q's forecast EPS (FEPS = current FY in progress)
    const qForecast = preferConsolidated ? toNumberOrNull(latestQ.FEPS) : toNumberOrNull(latestQ.FNCEPS);

    if (qForecast === null) return;

    const roundedQForecast = Math.round(qForecast * 100) / 100;

    // Annotate when Q forecast differs from FY forecast, or FY has no forecast at all
    if (latestFY.forecastEps === null || roundedQForecast !== latestFY.forecastEps) {
      data[latestFyIdx] = {
        ...latestFY,
        revisedForecastEps: roundedQForecast,
        revisedForecastSource: normalizePeriodType(latestQ.CurPerType) ?? latestQ.CurPerType,
      };
    }
  }

  /**
   * Get forecast EPS and calculate change rate from actual EPS
   */
  private getForecastEps(
    statement: JQuantsStatement | undefined,
    actualEps: number | null,
    preferConsolidated: boolean
  ): { forecastEps: number | null; forecastEpsChangeRate: number | null } {
    if (!statement) {
      return { forecastEps: null, forecastEpsChangeRate: null };
    }

    // FY: NxFEPS priority (next FY forecast = forward-looking)
    // Q: FEPS priority (current FY forecast = forward-looking)
    let forecastEps: number | null = null;
    const isFY = isFiscalYear(statement.CurPerType);

    if (preferConsolidated) {
      forecastEps = isFY
        ? (toNumberOrNull(statement.NxFEPS) ?? toNumberOrNull(statement.FEPS))
        : (toNumberOrNull(statement.FEPS) ?? toNumberOrNull(statement.NxFEPS));
    } else {
      forecastEps = isFY
        ? (toNumberOrNull(statement.NxFNCEPS) ?? toNumberOrNull(statement.FNCEPS))
        : (toNumberOrNull(statement.FNCEPS) ?? toNumberOrNull(statement.NxFNCEPS));
    }

    // Round to 2 decimal places
    forecastEps = forecastEps !== null ? Math.round(forecastEps * 100) / 100 : null;

    // Calculate change rate if both values are available
    let forecastEpsChangeRate: number | null = null;
    if (forecastEps !== null && actualEps !== null && actualEps !== 0) {
      forecastEpsChangeRate = Math.round(((forecastEps - actualEps) / Math.abs(actualEps)) * 100 * 100) / 100;
    }

    return { forecastEps, forecastEpsChangeRate };
  }

  /**
   * Get cash flow data from previous period (same period type, one year earlier)
   */
  private getPreviousPeriodCashFlow(
    currentDate: string,
    periodType: string,
    statements: JQuantsStatement[]
  ): {
    prevCashFlowOperating: number | null;
    prevCashFlowInvesting: number | null;
    prevCashFlowFinancing: number | null;
    prevCashAndEquivalents: number | null;
  } {
    // Calculate target date (approximately one year earlier)
    const currentDateObj = new Date(currentDate);
    const targetYear = currentDateObj.getFullYear() - 1;
    const targetMonth = currentDateObj.getMonth();
    const targetDay = currentDateObj.getDate();

    // Find statement with same period type from approximately one year earlier
    const prevStatement = statements.find((s) => {
      const normalizedPeriodType = normalizePeriodType(periodType);
      const stmtPeriodType = normalizePeriodType(s.CurPerType);
      if (stmtPeriodType !== normalizedPeriodType) return false;
      const stmtDate = new Date(s.CurPerEn);
      // Allow ±45 days tolerance for fiscal year end variations
      const daysDiff = Math.abs(
        (stmtDate.getTime() - new Date(targetYear, targetMonth, targetDay).getTime()) / (1000 * 60 * 60 * 24)
      );
      return daysDiff < 45;
    });

    if (!prevStatement) {
      return {
        prevCashFlowOperating: null,
        prevCashFlowInvesting: null,
        prevCashFlowFinancing: null,
        prevCashAndEquivalents: null,
      };
    }

    return {
      prevCashFlowOperating: this.toMillions(toNumberOrNull(prevStatement.CFO)),
      prevCashFlowInvesting: this.toMillions(toNumberOrNull(prevStatement.CFI)),
      prevCashFlowFinancing: this.toMillions(toNumberOrNull(prevStatement.CFF)),
      prevCashAndEquivalents: this.toMillions(toNumberOrNull(prevStatement.CashEq)),
    };
  }

  /**
   * Round to 2 decimal places or return null
   */
  private roundOrNull(value: number | null): number | null {
    return value !== null ? Math.round(value * 100) / 100 : null;
  }

  /**
   * Convert JPY to millions of JPY for display formatting.
   */
  private toMillions(value: number | null): number | null {
    if (value === null) return null;
    return value / 1_000_000;
  }

  /**
   * Close resources
   */
  close(): void {
    if (this.marketReader) {
      this.marketReader.close();
      this.marketReader = null;
    }
  }
}
