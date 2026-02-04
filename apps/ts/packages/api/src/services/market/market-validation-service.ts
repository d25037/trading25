import {
  MarketDatabase,
  METADATA_KEYS,
  type AdjustmentEvent as SharedAdjustmentEvent,
} from '@trading25/shared/market-sync';
import { getMarketDbPath } from '@trading25/shared/utils/dataset-paths';
import { logger } from '@trading25/shared/utils/logger';
import type { AdjustmentEvent, IntegrityIssue, MarketValidationResponse } from '../../schemas/market-validation';

/**
 * Interpret adjustment factor as event type
 */
function interpretAdjustmentFactor(adjustmentFactor: number): string {
  if (adjustmentFactor < 0.7) {
    const splitRatio = Math.round(1 / adjustmentFactor);
    return `1:${splitRatio} stock split`;
  }
  if (adjustmentFactor > 1.3) {
    const mergerRatio = Math.round(adjustmentFactor);
    return `${mergerRatio}:1 reverse split`;
  }
  return 'adjustment';
}

/**
 * Format date to YYYY-MM-DD string
 */
function formatDate(date: Date | null): string | null {
  if (!date) return null;
  return date.toISOString().split('T')[0] || null;
}

/**
 * Build recommendations based on database state
 */
function buildRecommendations(
  isInitialized: boolean,
  missingDatesCount: number,
  failedDatesCount: number,
  pendingRefresh: string[],
  integrityIssuesCount: number
): string[] {
  const recommendations: string[] = [];

  if (!isInitialized) {
    recommendations.push("Run 'bun cli market sync --init' to initialize the database");
    return recommendations;
  }

  if (missingDatesCount > 0 || failedDatesCount > 0) {
    recommendations.push("Run 'bun cli market sync' to fill missing data");
  }

  if (pendingRefresh.length > 0) {
    const codes = pendingRefresh.slice(0, 5).join(' ');
    recommendations.push(
      `${pendingRefresh.length} stocks need refresh for split adjustments: bun cli market refresh ${codes}`
    );
  }

  if (integrityIssuesCount > 0) {
    recommendations.push("Data integrity issue: Run 'bun cli market sync --init' to rebuild");
  }

  if (recommendations.length === 0) {
    recommendations.push('Database is complete and up to date');
  }

  return recommendations;
}

/**
 * Determine validation status based on issues found
 */
function determineStatus(
  isInitialized: boolean,
  missingDatesCount: number,
  failedDatesCount: number,
  pendingRefreshCount: number,
  integrityIssuesCount: number
): 'healthy' | 'warning' | 'error' {
  if (!isInitialized) {
    return 'error';
  }

  if (integrityIssuesCount > 0) {
    return 'error';
  }

  if (missingDatesCount > 0 || failedDatesCount > 0 || pendingRefreshCount > 0) {
    return 'warning';
  }

  return 'healthy';
}

/**
 * Transform adjustment events to API format
 */
function transformAdjustmentEvents(events: SharedAdjustmentEvent[], limit: number): AdjustmentEvent[] {
  return events.slice(0, limit).map((event) => ({
    code: event.code,
    date: formatDate(event.date) || '',
    adjustmentFactor: event.adjustmentFactor,
    close: event.close,
    eventType: interpretAdjustmentFactor(event.adjustmentFactor),
  }));
}

/**
 * Transform integrity issues to API format
 */
function transformIntegrityIssues(issues: Array<{ code: string; count: number }>, limit: number): IntegrityIssue[] {
  return issues.slice(0, limit).map((issue) => ({
    code: issue.code,
    count: issue.count,
  }));
}

/**
 * Format date range for response
 */
function formatDateRange(min: Date | null, max: Date | null): { min: string; max: string } | null {
  if (!min || !max) return null;
  return {
    min: formatDate(min) || '',
    max: formatDate(max) || '',
  };
}

export class MarketValidationService {
  private db: MarketDatabase | null = null;
  private dbPath: string;

  constructor() {
    this.dbPath = getMarketDbPath();
  }

  private getDatabase(): MarketDatabase {
    if (!this.db) {
      this.db = new MarketDatabase(this.dbPath);
    }
    return this.db;
  }

  async validate(): Promise<MarketValidationResponse> {
    logger.debug('Validating market database', { dbPath: this.dbPath });

    const db = this.getDatabase();

    // Get basic info
    const isInitialized = db.isInitialized();
    const lastSync = db.getMetadata(METADATA_KEYS.LAST_SYNC_DATE);
    const lastStocksRefresh = db.getMetadata(METADATA_KEYS.LAST_STOCKS_REFRESH);

    // Get database metrics
    const topixRange = db.getTopixDateRange();
    const stockCount = db.getStockCount();
    const stockDataRange = db.getStockDataDateRange();
    const allMissingDates = db.getMissingStockDataDates();
    const allFailedDates = db.getFailedDates();
    const allAdjustmentEvents = db.getAdjustmentEvents(50);
    const allIntegrityIssues = db.getStocksOutsideTopixRange();

    // Get stocks needing refresh
    const pendingRefresh = this.getPendingRefreshStocks(db);

    // Build response components
    const recommendations = buildRecommendations(
      isInitialized,
      allMissingDates.length,
      allFailedDates.length,
      pendingRefresh,
      allIntegrityIssues.length
    );

    const status = determineStatus(
      isInitialized,
      allMissingDates.length,
      allFailedDates.length,
      pendingRefresh.length,
      allIntegrityIssues.length
    );

    logger.debug('Validation complete', {
      status,
      initialized: isInitialized,
      stockCount: stockCount.total,
      missingDatesCount: allMissingDates.length,
    });

    return {
      status,
      initialized: isInitialized,
      lastSync: lastSync || null,
      lastStocksRefresh: lastStocksRefresh || null,
      topix: {
        count: topixRange.count,
        dateRange: formatDateRange(topixRange.min, topixRange.max),
      },
      stocks: {
        total: stockCount.total,
        byMarket: stockCount.byMarket,
      },
      stockData: {
        count: stockDataRange.count,
        dateRange: formatDateRange(stockDataRange.min, stockDataRange.max),
        missingDates: allMissingDates.slice(0, 20).map((d) => formatDate(d) || ''),
        missingDatesCount: allMissingDates.length,
      },
      failedDates: allFailedDates.slice(0, 10).map((d) => formatDate(d) || ''),
      failedDatesCount: allFailedDates.length,
      adjustmentEvents: transformAdjustmentEvents(allAdjustmentEvents, 20),
      adjustmentEventsCount: allAdjustmentEvents.length,
      stocksNeedingRefresh: pendingRefresh.slice(0, 20),
      stocksNeedingRefreshCount: pendingRefresh.length,
      integrityIssues: transformIntegrityIssues(allIntegrityIssues, 10),
      integrityIssuesCount: allIntegrityIssues.length,
      recommendations,
      lastUpdated: new Date().toISOString(),
    };
  }

  /**
   * Get stocks that need refresh (excluding already refetched)
   */
  private getPendingRefreshStocks(db: MarketDatabase): string[] {
    const allStocksNeedingRefresh = db.getStocksNeedingRefresh();
    const refetchedStocks = db.getRefreshedStocks();
    const refetchedCodes = new Set(refetchedStocks.map((item: { code: string; refetchedAt: string }) => item.code));
    return allStocksNeedingRefresh.filter((code: string) => !refetchedCodes.has(code));
  }

  close(): void {
    if (this.db) {
      this.db.close();
      this.db = null;
    }
  }
}
