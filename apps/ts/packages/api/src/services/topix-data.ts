import * as fs from 'node:fs';
import type { JQuantsClient, JQuantsTOPIXParams, JQuantsTOPIXResponse } from '@trading25/shared';
import { DrizzleMarketDataReader } from '@trading25/shared/db';
import { getMarketDbPath } from '@trading25/shared/utils/dataset-paths';
import { logger } from '@trading25/shared/utils/logger';
import { createJQuantsClient } from '../utils/jquants-client-factory';

interface TOPIXCacheEntry {
  data: JQuantsTOPIXResponse;
  timestamp: number;
  ttl: number;
}

export class TopixDataService {
  private cache = new Map<string, TOPIXCacheEntry>();
  private readonly cacheTTL = 60 * 60 * 1000; // 1 hour in milliseconds
  private jquantsClient: JQuantsClient | null = null;
  private static marketDbReader: DrizzleMarketDataReader | null = null;

  /**
   * Get market database reader (lazy initialization)
   * Returns null if market.db doesn't exist
   */
  private getMarketDbReader(): DrizzleMarketDataReader | null {
    if (TopixDataService.marketDbReader !== null) {
      return TopixDataService.marketDbReader;
    }

    const dbPath = getMarketDbPath();
    if (!fs.existsSync(dbPath)) {
      logger.debug('Market database not found, will use JQuants API for TOPIX', { dbPath });
      return null;
    }

    try {
      TopixDataService.marketDbReader = new DrizzleMarketDataReader(dbPath);
      logger.info('Market database reader initialized for TOPIX', { dbPath });
      return TopixDataService.marketDbReader;
    } catch (error) {
      logger.warn('Failed to initialize market database reader for TOPIX', {
        dbPath,
        error: error instanceof Error ? error.message : error,
      });
      return null;
    }
  }

  private getJQuantsClient(): JQuantsClient {
    if (!this.jquantsClient) {
      this.jquantsClient = createJQuantsClient();
    }
    return this.jquantsClient;
  }

  private generateCacheKey(params?: JQuantsTOPIXParams): string {
    if (!params) {
      return 'all';
    }

    const { from, to } = params;
    return `${from || ''}-${to || ''}`;
  }

  private isValidCacheEntry(entry: TOPIXCacheEntry): boolean {
    return Date.now() - entry.timestamp < entry.ttl;
  }

  async getTOPIXData(params?: JQuantsTOPIXParams): Promise<JQuantsTOPIXResponse> {
    const cacheKey = this.generateCacheKey(params);

    // Check cache first
    const cachedEntry = this.cache.get(cacheKey);
    if (cachedEntry && this.isValidCacheEntry(cachedEntry)) {
      logger.debug('TOPIX cache hit', { cacheKey });
      return cachedEntry.data;
    }

    // Try to get from market.db first
    const marketDbResult = this.getTopixDataFromMarketDb(params);
    if (marketDbResult) {
      // Cache the result
      this.cache.set(cacheKey, {
        data: marketDbResult,
        timestamp: Date.now(),
        ttl: this.cacheTTL,
      });
      logger.debug('Retrieved TOPIX data from market.db', { dataPoints: marketDbResult.data.length });
      return marketDbResult;
    }

    // Fallback to JQuants API
    logger.debug('Falling back to JQuants API for TOPIX', { cacheKey });
    return this.getTopixDataFromJQuantsApi(params, cacheKey);
  }

  /**
   * Get TOPIX data from market.db
   * Returns null if data not found or database not available
   */
  private getTopixDataFromMarketDb(params?: JQuantsTOPIXParams): JQuantsTOPIXResponse | null {
    const reader = this.getMarketDbReader();
    if (!reader) {
      return null;
    }

    try {
      // Build date range if params provided
      const dateRange =
        params?.from || params?.to
          ? {
              from: params.from ? new Date(params.from) : new Date('1900-01-01'),
              to: params.to ? new Date(params.to) : new Date('2100-12-31'),
            }
          : undefined;

      const topixRows = reader.getTopixData(dateRange);
      if (topixRows.length === 0) {
        logger.debug('No TOPIX data found in market.db');
        return null;
      }

      // Convert to JQuants response format
      const data = topixRows.map((row) => ({
        Date: row.date.toISOString().split('T')[0] as string,
        O: row.open,
        H: row.high,
        L: row.low,
        C: row.close,
      }));

      return { data };
    } catch (error) {
      logger.warn('Error reading TOPIX from market.db, will fallback to API', {
        error: error instanceof Error ? error.message : error,
      });
      return null;
    }
  }

  /**
   * Get TOPIX data from JQuants API (fallback)
   */
  private async getTopixDataFromJQuantsApi(
    params: JQuantsTOPIXParams | undefined,
    cacheKey: string
  ): Promise<JQuantsTOPIXResponse> {
    logger.debug('Fetching TOPIX data from JQuants API', { cacheKey });

    try {
      const client = this.getJQuantsClient();
      const data = await client.getTOPIX(params);

      // Cache the result
      this.cache.set(cacheKey, {
        data,
        timestamp: Date.now(),
        ttl: this.cacheTTL,
      });

      // Clean up expired cache entries
      this.cleanupCache();

      logger.debug('Successfully fetched and cached TOPIX data from JQuants API', {
        dataPoints: data.data?.length || 0,
      });
      return data;
    } catch (error) {
      logger.error('Error fetching TOPIX data', { error: error instanceof Error ? error.message : String(error) });
      throw new Error(`Failed to fetch TOPIX data: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }

  private cleanupCache(): void {
    const now = Date.now();
    let removedCount = 0;

    for (const [key, entry] of this.cache.entries()) {
      if (now - entry.timestamp >= entry.ttl) {
        this.cache.delete(key);
        removedCount++;
      }
    }

    if (removedCount > 0) {
      logger.debug('Cleaned up expired TOPIX cache entries', { removedCount });
    }
  }

  getCacheStats(): { size: number; keys: string[] } {
    return {
      size: this.cache.size,
      keys: Array.from(this.cache.keys()),
    };
  }

  clearCache(): void {
    this.cache.clear();
    logger.info('TOPIX cache cleared');
  }
}
