import { MarketDataReader, type RankingItem as SharedRankingItem } from '@trading25/shared/market-sync';
import { getMarketDbPath } from '@trading25/shared/utils/dataset-paths';
import { logger } from '@trading25/shared/utils/logger';
import type { MarketRankingResponse, RankingItem } from '../../schemas/market-ranking';

interface RankingQueryOptions {
  date?: string;
  limit: number;
  markets: string;
  lookbackDays: number;
  periodDays: number;
}

export class MarketRankingService {
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

  async getRankings(options: RankingQueryOptions): Promise<MarketRankingResponse> {
    logger.debug('Getting market rankings', { options });

    const reader = this.getReader();

    // Parse market codes
    const marketCodes = options.markets.split(',').map((m) => m.trim());

    // Determine target date
    let targetDate: Date;
    if (options.date) {
      targetDate = new Date(options.date);
    } else {
      const latestDate = reader.getLatestTradingDate();
      if (!latestDate) {
        throw new Error('No trading data available in database');
      }
      targetDate = latestDate;
    }

    logger.debug('Using target date', { targetDate: targetDate.toISOString() });

    // Fetch rankings
    let tradingValueItems: SharedRankingItem[];
    if (options.lookbackDays > 1) {
      tradingValueItems = reader.getRankingByTradingValueAverage(
        targetDate,
        options.lookbackDays,
        options.limit,
        marketCodes
      );
    } else {
      tradingValueItems = reader.getRankingByTradingValue(targetDate, options.limit, marketCodes);
    }

    let gainersItems: SharedRankingItem[];
    let losersItems: SharedRankingItem[];
    if (options.lookbackDays > 1) {
      gainersItems = reader.getRankingByPriceChangeFromDays(
        targetDate,
        options.lookbackDays,
        options.limit,
        marketCodes,
        'gainers'
      );
      losersItems = reader.getRankingByPriceChangeFromDays(
        targetDate,
        options.lookbackDays,
        options.limit,
        marketCodes,
        'losers'
      );
    } else {
      gainersItems = reader.getRankingByPriceChange(targetDate, options.limit, marketCodes, 'gainers');
      losersItems = reader.getRankingByPriceChange(targetDate, options.limit, marketCodes, 'losers');
    }

    // Fetch period high/low rankings
    const periodHighItems = reader.getRankingByPeriodHigh(targetDate, options.periodDays, options.limit, marketCodes);
    const periodLowItems = reader.getRankingByPeriodLow(targetDate, options.periodDays, options.limit, marketCodes);

    logger.debug('Rankings fetched', {
      tradingValueCount: tradingValueItems.length,
      gainersCount: gainersItems.length,
      losersCount: losersItems.length,
      periodHighCount: periodHighItems.length,
      periodLowCount: periodLowItems.length,
    });

    // Transform to API format
    const dateStr = targetDate.toISOString().split('T')[0] || '';

    return {
      date: dateStr,
      markets: marketCodes,
      lookbackDays: options.lookbackDays,
      periodDays: options.periodDays,
      rankings: {
        tradingValue: tradingValueItems.map((item) => this.transformRankingItem(item)),
        gainers: gainersItems.map((item) => this.transformRankingItem(item)),
        losers: losersItems.map((item) => this.transformRankingItem(item)),
        periodHigh: periodHighItems.map((item) => this.transformRankingItem(item)),
        periodLow: periodLowItems.map((item) => this.transformRankingItem(item)),
      },
      lastUpdated: new Date().toISOString(),
    };
  }

  private transformRankingItem(item: SharedRankingItem): RankingItem {
    return {
      rank: item.rank,
      code: item.code,
      companyName: item.companyName,
      marketCode: item.marketCode,
      sector33Name: item.sector33Name,
      currentPrice: item.currentPrice,
      volume: item.volume,
      tradingValue: item.tradingValue,
      tradingValueAverage: item.tradingValueAverage,
      previousPrice: item.previousPrice,
      basePrice: item.basePrice,
      changeAmount: item.changeAmount,
      changePercentage: item.changePercentage,
      lookbackDays: item.lookbackDays,
    };
  }

  close(): void {
    if (this.reader) {
      this.reader.close();
      this.reader = null;
    }
  }
}
