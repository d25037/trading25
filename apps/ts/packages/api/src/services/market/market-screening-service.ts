import type {
  FilterCriteria,
  ScreeningConfig,
  ScreeningInput,
  ScreeningResult,
  StockDataPoint,
} from '@trading25/shared';
import { DEFAULT_SCREENING_CONFIG, ScreeningEngine } from '@trading25/shared';
import { MarketDataReader } from '@trading25/shared/market-sync';
import { getMarketDbPath } from '@trading25/shared/utils/dataset-paths';
import { logger } from '@trading25/shared/utils/logger';
import type {
  FuturePricePoint,
  FutureReturns,
  MarketScreeningResponse,
  ScreeningResultItem,
} from '../../schemas/market-screening';

interface ScreeningQueryOptions {
  markets: string;
  rangeBreakFast: boolean;
  rangeBreakSlow: boolean;
  recentDays: number;
  referenceDate?: string;
  minBreakPercentage?: number;
  minVolumeRatio?: number;
  sortBy: 'date' | 'stockCode' | 'volumeRatio' | 'breakPercentage';
  order: 'asc' | 'desc';
  limit?: number;
}

/**
 * Stock info from database
 */
interface StockListItem {
  code: string;
  companyName: string;
  scaleCategory: string;
  sector33Name: string;
}

/**
 * Convert stock data from market.db format to screening format
 */
function convertStockData(
  stockData: { date: Date; open: number; high: number; low: number; close: number; volume: number }[]
): StockDataPoint[] {
  return stockData.map((data) => ({
    date: data.date,
    open: data.open,
    high: data.high,
    low: data.low,
    close: data.close,
    volume: data.volume,
  }));
}

/**
 * Format date to YYYY-MM-DD string
 */
function formatDate(date: Date): string {
  return date.toISOString().split('T')[0] || '';
}

export class MarketScreeningService {
  private reader: MarketDataReader | null = null;
  private dbPath: string;

  constructor() {
    this.dbPath = getMarketDbPath();
  }

  private getReader(): MarketDataReader {
    if (!this.reader) {
      this.reader = new MarketDataReader(this.dbPath);
    }
    return this.reader;
  }

  async runScreening(options: ScreeningQueryOptions): Promise<MarketScreeningResponse> {
    logger.debug('Running market screening', { options });

    const reader = this.getReader();

    // Parse market codes
    const marketCodes = options.markets.split(',').map((m) => m.trim());

    // Create screening config
    const config: ScreeningConfig = {
      ...DEFAULT_SCREENING_CONFIG,
      rangeBreakFastEnabled: options.rangeBreakFast,
      rangeBreakSlowEnabled: options.rangeBreakSlow,
      recentDays: options.recentDays,
    };

    // Load stocks
    const stocks = reader.getStockList(marketCodes);
    logger.debug('Loaded stocks', { count: stocks.length, markets: marketCodes });

    if (stocks.length === 0) {
      return this.createEmptyResponse(marketCodes, options.recentDays, options.referenceDate);
    }

    // Create engine
    const engine = new ScreeningEngine(config);

    // Prepare inputs (with optional date filtering for historical screening)
    const { inputs, skippedCount, fullDataMap } = this.prepareInputs(stocks, reader, engine, options.referenceDate);
    logger.debug('Prepared screening inputs', { count: inputs.length, skipped: skippedCount });

    if (inputs.length === 0) {
      return this.createEmptyResponse(
        marketCodes,
        options.recentDays,
        options.referenceDate,
        stocks.length,
        skippedCount
      );
    }

    // Run screening
    const results = await engine.screenMultipleStocks(inputs);
    logger.debug('Screening complete', { matchCount: results.length });

    // Apply filters
    const filteredResults = this.applyFilters(results, options);

    // Transform results (with future returns calculation for historical screening)
    const transformedResults = filteredResults.map((r) =>
      this.transformResult(r, options.referenceDate ? fullDataMap.get(r.stockCode) : undefined)
    );

    // Count by type
    const rangeBreakFastCount = results.filter((r) => r.screeningType === 'rangeBreakFast').length;
    const rangeBreakSlowCount = results.filter((r) => r.screeningType === 'rangeBreakSlow').length;

    return {
      results: transformedResults,
      summary: {
        totalStocksScreened: inputs.length,
        matchCount: results.length,
        skippedCount,
        byScreeningType: {
          rangeBreakFast: rangeBreakFastCount,
          rangeBreakSlow: rangeBreakSlowCount,
        },
      },
      markets: marketCodes,
      recentDays: options.recentDays,
      referenceDate: options.referenceDate,
      lastUpdated: new Date().toISOString(),
    };
  }

  /**
   * Apply date filtering for historical screening
   * Returns filtered data or null if stock should be skipped
   */
  private filterDataByReferenceDate(convertedData: StockDataPoint[], referenceDate: string): StockDataPoint[] | null {
    const refIndex = convertedData.findIndex((d) => formatDate(d.date) === referenceDate);

    if (refIndex !== -1) {
      // Truncate data to include only up to reference date (inclusive)
      return convertedData.slice(0, refIndex + 1);
    }

    // Reference date not found in data, find closest date before it
    const refDateMs = new Date(referenceDate).getTime();
    const closestIndex = convertedData.findIndex((d) => d.date.getTime() > refDateMs);

    if (closestIndex === -1) {
      // All data is before reference date, use all
      return convertedData;
    }

    if (closestIndex === 0) {
      // All data is after reference date, skip this stock
      return null;
    }

    // Truncate at the point before reference date
    return convertedData.slice(0, closestIndex);
  }

  private prepareInputs(
    stocks: StockListItem[],
    reader: MarketDataReader,
    engine: ScreeningEngine,
    referenceDate?: string
  ): { inputs: ScreeningInput[]; skippedCount: number; fullDataMap: Map<string, StockDataPoint[]> } {
    const inputs: ScreeningInput[] = [];
    const fullDataMap = new Map<string, StockDataPoint[]>();
    let skippedCount = 0;

    for (const stock of stocks) {
      try {
        const stockData = reader.getStockData(stock.code);

        if (stockData.length === 0) {
          skippedCount++;
          continue;
        }

        const convertedData = convertStockData(stockData);

        // Store full data for future returns calculation (only when historical screening)
        if (referenceDate) {
          fullDataMap.set(stock.code, convertedData);
        }

        // Apply date filtering for historical screening
        const filteredData = referenceDate
          ? this.filterDataByReferenceDate(convertedData, referenceDate)
          : convertedData;

        if (!filteredData) {
          skippedCount++;
          continue;
        }

        const input: ScreeningInput = {
          stockCode: stock.code,
          companyName: stock.companyName,
          scaleCategory: stock.scaleCategory,
          sector33Name: stock.sector33Name,
          data: filteredData,
        };

        const validation = engine.validateInput(input);
        if (!validation.isValid) {
          skippedCount++;
          continue;
        }

        inputs.push(input);
      } catch {
        skippedCount++;
      }
    }

    return { inputs, skippedCount, fullDataMap };
  }

  private applyFilters(results: ScreeningResult[], options: ScreeningQueryOptions): ScreeningResult[] {
    const criteria: FilterCriteria = {};

    if (options.minBreakPercentage !== undefined) {
      criteria.minBreakPercentage = options.minBreakPercentage;
    }

    if (options.minVolumeRatio !== undefined) {
      criteria.minVolumeRatio = options.minVolumeRatio;
    }

    let filtered = ScreeningEngine.filterResults(results, criteria);
    filtered = ScreeningEngine.sortResults(filtered, options.sortBy, options.order);

    if (options.limit !== undefined && options.limit > 0) {
      filtered = filtered.slice(0, options.limit);
    }

    return filtered;
  }

  private transformResult(result: ScreeningResult, fullData?: StockDataPoint[]): ScreeningResultItem {
    const item: ScreeningResultItem = {
      stockCode: result.stockCode,
      companyName: result.companyName,
      scaleCategory: result.scaleCategory,
      sector33Name: result.sector33Name,
      screeningType: result.screeningType,
      matchedDate: formatDate(result.matchedDate),
      details: {},
    };

    if (result.details.rangeBreak) {
      item.details.rangeBreak = {
        breakDate: formatDate(result.details.rangeBreak.breakDate),
        currentHigh: result.details.rangeBreak.currentHigh,
        maxHighInLookback: result.details.rangeBreak.maxHighInLookback,
        breakPercentage: result.details.rangeBreak.breakPercentage,
        volumeRatio: result.details.rangeBreak.volumeRatio,
        avgVolume20Days: result.details.rangeBreak.avgVolume20Days,
        avgVolume100Days: result.details.rangeBreak.avgVolume100Days,
      };
    }

    // Calculate future returns for historical screening
    if (fullData) {
      item.futureReturns = this.calculateFutureReturns(fullData, result.matchedDate);
    }

    return item;
  }

  /**
   * Calculate future price returns at fixed offsets (5, 20, 60 days)
   */
  private calculateFutureReturns(fullData: StockDataPoint[], breakDate: Date): FutureReturns {
    const breakDateStr = formatDate(breakDate);
    const breakIndex = fullData.findIndex((d) => formatDate(d.date) === breakDateStr);

    if (breakIndex === -1) {
      return { day5: null, day20: null, day60: null };
    }

    const breakPrice = fullData[breakIndex]?.close;
    if (!breakPrice || breakPrice === 0) {
      return { day5: null, day20: null, day60: null };
    }

    return {
      day5: this.getPricePointAtOffset(fullData, breakIndex, 5, breakPrice),
      day20: this.getPricePointAtOffset(fullData, breakIndex, 20, breakPrice),
      day60: this.getPricePointAtOffset(fullData, breakIndex, 60, breakPrice),
    };
  }

  /**
   * Get price point at specified offset from break index
   */
  private getPricePointAtOffset(
    data: StockDataPoint[],
    breakIndex: number,
    offset: number,
    breakPrice: number
  ): FuturePricePoint | null {
    const targetIndex = breakIndex + offset;

    if (targetIndex >= data.length) {
      return null;
    }

    const targetData = data[targetIndex];
    if (!targetData) {
      return null;
    }

    const price = targetData.close;
    const changePercent = ((price - breakPrice) / breakPrice) * 100;

    return {
      date: formatDate(targetData.date),
      price,
      changePercent: Math.round(changePercent * 100) / 100,
    };
  }

  private createEmptyResponse(
    markets: string[],
    recentDays: number,
    referenceDate?: string,
    totalStocks = 0,
    skippedCount = 0
  ): MarketScreeningResponse {
    return {
      results: [],
      summary: {
        totalStocksScreened: totalStocks - skippedCount,
        matchCount: 0,
        skippedCount,
        byScreeningType: {
          rangeBreakFast: 0,
          rangeBreakSlow: 0,
        },
      },
      markets,
      recentDays,
      referenceDate,
      lastUpdated: new Date().toISOString(),
    };
  }

  close(): void {
    if (this.reader) {
      this.reader.close();
      this.reader = null;
    }
  }
}
