import * as fs from 'node:fs';
// margin-volume-ratio: apps/bt/ API proxy に移行済み（longMarginVolumeRatio, shortMarginVolumeRatio は不要）
import { DrizzleMarketDataReader, type StockSearchResult } from '@trading25/shared/db';
import type {
  ApiMarginFlowPressureData,
  ApiMarginLongPressureData,
  ApiMarginPressureIndicatorsResponse,
  ApiMarginTurnoverDaysData,
  ApiMarginVolumeRatioResponse,
  ApiStockDataPoint,
  ApiStockDataResponse,
} from '@trading25/shared/types/api-types';
import type { JQuantsDailyQuotesResponse } from '@trading25/shared/types/jquants';
import { getMarketDbPath } from '@trading25/shared/utils/dataset-paths';
import { logger } from '@trading25/shared/utils/logger';
import { BaseJQuantsService } from './base-jquants-service';

interface CacheEntry<T> {
  data: T;
  timestamp: number;
}

const DAILY_QUOTES_CACHE_TTL_MS = 60 * 1000; // 1 minute

export class StockDataService extends BaseJQuantsService {
  // Static caches shared across all instances (routes create separate instances)
  private static companyNameCache: Map<string, string> = new Map();
  private static dailyQuotesCache: Map<string, CacheEntry<JQuantsDailyQuotesResponse>> = new Map();
  private static marketDbReader: DrizzleMarketDataReader | null = null;

  /**
   * Get market database reader (lazy initialization)
   * Returns null if market.db doesn't exist
   */
  private getMarketDbReader(): DrizzleMarketDataReader | null {
    if (StockDataService.marketDbReader !== null) {
      return StockDataService.marketDbReader;
    }

    const dbPath = getMarketDbPath();
    if (!fs.existsSync(dbPath)) {
      logger.debug('Market database not found, will use JQuants API', { dbPath });
      return null;
    }

    try {
      StockDataService.marketDbReader = new DrizzleMarketDataReader(dbPath);
      logger.info('Market database reader initialized', { dbPath });
      return StockDataService.marketDbReader;
    } catch (error) {
      logger.warn('Failed to initialize market database reader', {
        dbPath,
        error: error instanceof Error ? error.message : error,
      });
      return null;
    }
  }

  /**
   * Get daily quotes with caching to avoid duplicate API calls
   * Cache TTL: 1 minute (sufficient for same-request-cycle deduplication)
   * Cache is shared across all StockDataService instances
   */
  private async getDailyQuotesWithCache(symbol: string): Promise<JQuantsDailyQuotesResponse> {
    const now = Date.now();
    const cached = StockDataService.dailyQuotesCache.get(symbol);

    if (cached && now - cached.timestamp < DAILY_QUOTES_CACHE_TTL_MS) {
      logger.debug('Using cached daily quotes', { symbol, cacheAge: now - cached.timestamp });
      return cached.data;
    }

    logger.debug('Fetching daily quotes from JQuants API', { symbol });
    const client = this.getJQuantsClient();
    const response = await this.withTokenRefresh(() => client.getDailyQuotes({ code: symbol }));

    // Cache the response
    StockDataService.dailyQuotesCache.set(symbol, { data: response, timestamp: now });

    // Clean up old cache entries (keep cache size manageable)
    if (StockDataService.dailyQuotesCache.size > 100) {
      const oldestAllowed = now - DAILY_QUOTES_CACHE_TTL_MS;
      for (const [key, entry] of StockDataService.dailyQuotesCache) {
        if (entry.timestamp < oldestAllowed) {
          StockDataService.dailyQuotesCache.delete(key);
        }
      }
    }

    return response;
  }

  async getStockData(symbol: string, timeframe: string, useAdjusted = true): Promise<ApiStockDataResponse> {
    logger.debug('Fetching stock data', { symbol, timeframe });

    // Try to get data from market.db first
    const marketDbResult = this.getStockDataFromMarketDb(symbol);
    if (marketDbResult) {
      logger.debug('Retrieved stock data from market.db', {
        symbol,
        dataPoints: marketDbResult.data.length,
      });
      return marketDbResult;
    }

    // Fallback to JQuants API
    logger.debug('Falling back to JQuants API', { symbol });
    return this.getStockDataFromJQuantsApi(symbol, timeframe, useAdjusted);
  }

  /**
   * Get stock data from market.db
   * Returns null if data not found or database not available
   */
  private getStockDataFromMarketDb(symbol: string): ApiStockDataResponse | null {
    const reader = this.getMarketDbReader();
    if (!reader) {
      return null;
    }

    try {
      // Get stock info for company name
      const stockInfo = reader.getStockByCode(symbol);
      if (!stockInfo) {
        logger.debug('Stock not found in market.db', { symbol });
        return null;
      }

      // Get stock data (already adjusted in market.db)
      const stockDataRows = reader.getStockData(symbol);
      if (stockDataRows.length === 0) {
        logger.debug('No stock data found in market.db', { symbol });
        return null;
      }

      // Convert to API format
      const data: ApiStockDataPoint[] = stockDataRows.map((row) => ({
        time: row.date.toISOString().split('T')[0] as string,
        open: row.open,
        high: row.high,
        low: row.low,
        close: row.close,
        volume: row.volume,
      }));

      // Cache company name for future use
      StockDataService.companyNameCache.set(symbol, stockInfo.companyName);

      return {
        symbol,
        companyName: stockInfo.companyName,
        timeframe: 'daily',
        data,
        lastUpdated: new Date().toISOString(),
      };
    } catch (error) {
      logger.warn('Error reading from market.db, will fallback to API', {
        symbol,
        error: error instanceof Error ? error.message : error,
      });
      return null;
    }
  }

  /**
   * Get stock data from JQuants API (fallback)
   */
  private async getStockDataFromJQuantsApi(
    symbol: string,
    timeframe: string,
    useAdjusted: boolean
  ): Promise<ApiStockDataResponse> {
    const response = await this.getDailyQuotesWithCache(symbol);

    if (!response.data || response.data.length === 0) {
      throw new Error(`No data found for symbol: ${symbol}`);
    }

    // JQuantsのデータをフロントエンドの形式に変換（Adjustmentデータを優先使用）
    const stockData: ApiStockDataPoint[] = response.data
      .filter((quote) => {
        // 調整後データがある場合はそれを、なければ通常データをチェック
        const hasAdjustedData =
          quote.AdjO !== null && quote.AdjH !== null && quote.AdjL !== null && quote.AdjC !== null;
        const hasRegularData = quote.O !== null && quote.H !== null && quote.L !== null && quote.C !== null;
        return hasAdjustedData || hasRegularData;
      })
      .map((quote) => {
        // useAdjustedフラグと調整後データの存在をチェック
        const hasAdjustedData =
          quote.AdjO !== null && quote.AdjH !== null && quote.AdjL !== null && quote.AdjC !== null;
        const shouldUseAdjusted = useAdjusted && hasAdjustedData;

        return {
          time: quote.Date,
          open: shouldUseAdjusted ? (quote.AdjO as number) : (quote.O as number),
          high: shouldUseAdjusted ? (quote.AdjH as number) : (quote.H as number),
          low: shouldUseAdjusted ? (quote.AdjL as number) : (quote.L as number),
          close: shouldUseAdjusted ? (quote.AdjC as number) : (quote.C as number),
          volume: shouldUseAdjusted ? quote.AdjVo || 0 : quote.Vo || 0,
        };
      });

    logger.debug('Retrieved stock data from JQuants API', {
      symbol,
      dataPoints: stockData.length,
    });

    // Fetch company name from dataset
    const companyName = await this.getCompanyName(symbol);

    return {
      symbol,
      companyName,
      timeframe,
      data: stockData,
      lastUpdated: new Date().toISOString(),
    };
  }

  private async getCompanyName(symbol: string): Promise<string | undefined> {
    try {
      // Check cache first (static cache shared across instances)
      if (StockDataService.companyNameCache.has(symbol)) {
        return StockDataService.companyNameCache.get(symbol);
      }

      // Fetch from JQuants API with token refresh
      logger.debug('Fetching company name from JQuants API', { symbol });
      const client = this.getJQuantsClient();
      const response = await this.withTokenRefresh(() => client.getListedInfo({ code: symbol }));

      if (response.data && response.data.length > 0 && response.data[0]) {
        const companyName = response.data[0].CoName;
        // Cache the result
        StockDataService.companyNameCache.set(symbol, companyName);
        logger.debug('Found company name', { symbol, companyName });
        return companyName;
      }

      logger.debug('No company name found for symbol', { symbol });
      return undefined;
    } catch (error) {
      logger.warn('Failed to fetch company name from JQuants API', {
        symbol,
        error: error instanceof Error ? error.message : error,
      });
      return undefined;
    }
  }

  /**
   * Post request to apps/bt/ API margin endpoint with timeout handling
   */
  private async postBtMarginApi<T>(body: Record<string, unknown>, operationName: string): Promise<T> {
    const btApiUrl = process.env.BT_API_URL ?? 'http://localhost:3002';
    const timeoutMs = 30_000;
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

    let response: Response;
    try {
      response = await fetch(`${btApiUrl}/api/indicators/margin`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
    } catch (error) {
      if (controller.signal.aborted) {
        throw new Error(`apps/bt/ API ${operationName} timed out after ${timeoutMs}ms`);
      }
      throw error;
    } finally {
      clearTimeout(timeoutId);
    }

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`apps/bt/ API ${operationName} failed (${response.status}): ${errorText}`);
    }

    return response.json() as Promise<T>;
  }

  async getMarginVolumeRatio(symbol: string): Promise<ApiMarginVolumeRatioResponse> {
    logger.debug('Fetching margin volume ratio from apps/bt/ API', { symbol });

    const result = await this.postBtMarginApi<{
      stock_code: string;
      indicators: {
        margin_volume_ratio?: Array<{
          date: string;
          longRatio: number;
          shortRatio: number;
          longVol: number;
          shortVol: number;
          weeklyAvgVolume: number;
        }>;
      };
    }>({ stock_code: symbol, indicators: ['margin_volume_ratio'] }, 'margin volume ratio');

    const ratioData = result.indicators.margin_volume_ratio ?? [];

    const longRatio = ratioData.map((item) => ({
      date: item.date,
      ratio: item.longRatio,
      weeklyAvgVolume: item.weeklyAvgVolume,
      marginVolume: item.longVol,
    }));

    const shortRatio = ratioData.map((item) => ({
      date: item.date,
      ratio: item.shortRatio,
      weeklyAvgVolume: item.weeklyAvgVolume,
      marginVolume: item.shortVol,
    }));

    logger.debug('Retrieved margin volume ratio from apps/bt/ API', {
      symbol,
      longRatioCount: longRatio.length,
      shortRatioCount: shortRatio.length,
    });

    return {
      symbol,
      longRatio,
      shortRatio,
      lastUpdated: new Date().toISOString(),
    };
  }

  async getMarginPressureIndicators(symbol: string, averagePeriod = 15): Promise<ApiMarginPressureIndicatorsResponse> {
    logger.debug('Fetching margin pressure indicators from apps/bt/ API', { symbol, averagePeriod });

    const result = await this.postBtMarginApi<{
      stock_code: string;
      indicators: {
        margin_long_pressure?: ApiMarginLongPressureData[];
        margin_flow_pressure?: ApiMarginFlowPressureData[];
        margin_turnover_days?: ApiMarginTurnoverDaysData[];
      };
    }>(
      {
        stock_code: symbol,
        indicators: ['margin_long_pressure', 'margin_flow_pressure', 'margin_turnover_days'],
        average_period: averagePeriod,
      },
      'margin indicators'
    );

    const longPressure = result.indicators.margin_long_pressure ?? [];
    const flowPressure = result.indicators.margin_flow_pressure ?? [];
    const turnoverDays = result.indicators.margin_turnover_days ?? [];

    logger.debug('Retrieved margin pressure indicators from apps/bt/ API', {
      symbol,
      averagePeriod,
      longPressureCount: longPressure.length,
      flowPressureCount: flowPressure.length,
      turnoverDaysCount: turnoverDays.length,
    });

    return {
      symbol,
      averagePeriod,
      longPressure,
      flowPressure,
      turnoverDays,
      lastUpdated: new Date().toISOString(),
    };
  }

  /**
   * Search stocks by code or company name (fuzzy search)
   * Uses market.db for search - returns empty array if database not available
   */
  searchStocks(query: string, limit = 20): StockSearchResult[] {
    const reader = this.getMarketDbReader();
    if (!reader) {
      logger.warn('Market database not available for stock search');
      return [];
    }

    try {
      const results = reader.searchStocks(query, limit);
      logger.debug('Stock search completed', {
        query,
        limit,
        resultCount: results.length,
      });
      return results;
    } catch (error) {
      logger.error('Stock search failed', {
        query,
        error: error instanceof Error ? error.message : error,
      });
      return [];
    }
  }
}
