/**
 * Dataset Data Service
 * Provides data access for dataset/*.db files
 * Used by Python API clients (trading25-bt)
 *
 * Note: Timeframe変換(weekly/monthly)はbt/ APIに委譲
 * 仕様: apps/bt/docs/spec-timeframe-resample.md
 */
import { BacktestClient, type OHLCVRecord as BtOHLCVRecord } from '@trading25/shared/clients/backtest';
import { DatasetReader } from '@trading25/shared/dataset';
import { INDEX_MASTER_DATA } from '@trading25/shared/db/constants/index-master-data';
import { INDEX_CATEGORIES } from '@trading25/shared/db/schema/market-schema';
import { hasActualFinancialData, isFiscalYear } from '@trading25/shared/fundamental-analysis';
import { getDatasetPath } from '@trading25/shared/utils/dataset-paths';
import { logger } from '@trading25/shared/utils/logger';
import type {
  BatchMarginQuery,
  BatchMarginResponse,
  BatchOHLCVQuery,
  BatchOHLCVResponse,
  BatchStatementsQuery,
  BatchStatementsResponse,
  DateRangeQuery,
  IndexListItem,
  IndexListQuery,
  MarginListItem,
  MarginListQuery,
  MarginRecord,
  OHLCRecord,
  OHLCVQuery,
  OHLCVRecord,
  PeriodType,
  SectorMappingRecord,
  SectorWithCountRecord,
  StatementsQuery,
  StatementsRecord,
  StockListItem,
  StockListQuery,
  StockSectorMappingItem,
  Timeframe,
} from '../../schemas/dataset-data';

/**
 * Convert date string to Date object with validation
 */
function parseDate(dateStr: string | undefined): Date | undefined {
  if (!dateStr) return undefined;
  const date = new Date(dateStr);
  if (Number.isNaN(date.getTime())) {
    throw new Error(`Invalid date string: ${dateStr}`);
  }
  return date;
}

/**
 * Format date to YYYY-MM-DD string
 */
function formatDate(date: Date): string {
  return date.toISOString().split('T')[0] ?? '';
}

/**
 * Build date range from query parameters
 */
function buildDateRange(startDate?: string, endDate?: string): { from: Date; to: Date } | undefined {
  if (!startDate && !endDate) return undefined;
  return {
    from: parseDate(startDate) ?? new Date('1900-01-01'),
    to: parseDate(endDate) ?? new Date(),
  };
}

/** Internal type for raw OHLCV data from DatasetReader */
interface RawOHLCVData {
  date: Date;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

/**
 * apps/bt/ APIクライアント（遅延初期化でテスタビリティを維持）
 * Note: テスト時のモック差し替えを可能にするため、遅延初期化パターンを使用
 */
let _btClient: BacktestClient | null = null;

function getBtClient(): BacktestClient {
  if (_btClient === null) {
    _btClient = new BacktestClient({
      baseUrl: process.env.BT_API_URL ?? 'http://localhost:3002',
      timeout: 30000,
    });
  }
  return _btClient;
}

/**
 * Convert apps/bt/ API response to OHLCVRecord[]
 */
function btOHLCVToRecords(data: BtOHLCVRecord[]): OHLCVRecord[] {
  return data.map((d) => ({
    date: d.date,
    open: d.open,
    high: d.high,
    low: d.low,
    close: d.close,
    volume: d.volume,
  }));
}

/**
 * apps/bt/ APIを使用してOHLCVをリサンプル
 * @returns リサンプルされたOHLCVレコード、またはエラー時null
 */
async function resampleOHLCVviaBt(
  stockCode: string,
  timeframe: 'weekly' | 'monthly',
  startDate?: string,
  endDate?: string
): Promise<OHLCVRecord[] | null> {
  try {
    const response = await getBtClient().resampleOHLCV({
      stock_code: stockCode,
      source: 'market', // datasetからのデータはmarket扱い
      timeframe,
      start_date: startDate,
      end_date: endDate,
    });
    return btOHLCVToRecords(response.data);
  } catch (error) {
    logger.error('Failed to resample OHLCV via apps/bt/ API', { stockCode, timeframe, error });
    return null;
  }
}

/**
 * Convert raw OHLCV data to OHLCVRecord response format
 */
function toOHLCVRecordsFromRaw(data: RawOHLCVData[]): OHLCVRecord[] {
  return data.map((d) => ({
    date: formatDate(d.date),
    open: d.open,
    high: d.high,
    low: d.low,
    close: d.close,
    volume: d.volume,
  }));
}

/**
 * Process items in batches with concurrency control.
 * Each item is processed by the given handler; null results are filtered out.
 */
async function processBatched<TItem, TResult>(
  items: TItem[],
  handler: (item: TItem) => Promise<{ key: string; value: TResult } | null>,
  batchSize = 10
): Promise<Record<string, TResult>> {
  const result: Record<string, TResult> = {};

  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);
    const batchResults = await Promise.all(batch.map(handler));
    for (const entry of batchResults) {
      if (entry !== null) {
        result[entry.key] = entry.value;
      }
    }
  }

  return result;
}

/**
 * Get first and last date from sorted dates array safely
 */
function getDateBounds(sortedDates: Date[]): { startDate?: string; endDate?: string } {
  if (sortedDates.length === 0) {
    return { startDate: undefined, endDate: undefined };
  }
  const first = sortedDates[0];
  const last = sortedDates[sortedDates.length - 1];
  return {
    startDate: first ? formatDate(first) : undefined,
    endDate: last ? formatDate(last) : undefined,
  };
}

/**
 * Get dataset reader for a dataset name
 */
function getReader(datasetName: string): DatasetReader | null {
  try {
    // Normalize dataset name (add .db if missing)
    const normalizedName = datasetName.endsWith('.db') ? datasetName : `${datasetName}.db`;
    const dbPath = getDatasetPath(normalizedName);
    return new DatasetReader(dbPath);
  } catch (error) {
    logger.error('Failed to get dataset reader', { datasetName, error });
    return null;
  }
}

/**
 * Build detailed stock list with date ranges (helper function)
 */
async function buildDetailedStockList(
  reader: DatasetReader,
  stocks: { code: string }[],
  minRecords: number
): Promise<StockListItem[]> {
  const result: StockListItem[] = [];
  for (const stock of stocks) {
    const dateRange = await reader.getStockDateRange(stock.code);
    if (!dateRange) continue;

    const data = await reader.getStockData(stock.code);
    if (data.length < minRecords) continue;

    result.push({
      stockCode: stock.code,
      record_count: data.length,
      start_date: formatDate(dateRange.from),
      end_date: formatDate(dateRange.to),
    });
  }
  return result;
}

/**
 * Build simple stock list with minRecords filtering (helper function)
 * Filters out orphan stocks (stocks with no OHLCV data)
 */
async function buildSimpleStockList(
  reader: DatasetReader,
  stocks: { code: string }[],
  minRecords: number,
  limit?: number
): Promise<StockListItem[]> {
  const result: StockListItem[] = [];
  for (const stock of stocks) {
    const data = await reader.getStockData(stock.code);
    if (data.length < minRecords) continue;

    result.push({
      stockCode: stock.code,
      record_count: data.length,
    });

    if (limit && result.length >= limit) break;
  }
  return result;
}

/**
 * Build index map from sector data (helper function)
 */
function buildIndexMap(
  sectorData: { sectorCode: string; sectorName: string; date: Date }[]
): Map<string, { name: string; dates: Date[] }> {
  const indexMap = new Map<string, { name: string; dates: Date[] }>();
  for (const d of sectorData) {
    const existing = indexMap.get(d.sectorCode);
    if (existing) {
      existing.dates.push(d.date);
    } else {
      indexMap.set(d.sectorCode, { name: d.sectorName, dates: [d.date] });
    }
  }
  return indexMap;
}

/**
 * Filter and build index list items (helper function)
 */
function buildIndexListItems(
  indexMap: Map<string, { name: string; dates: Date[] }>,
  filterCodes: string[] | undefined,
  minRecords: number
): IndexListItem[] {
  const result: IndexListItem[] = [];
  for (const [code, info] of indexMap) {
    if (filterCodes && !filterCodes.includes(code)) continue;
    if (info.dates.length < minRecords) continue;

    const sortedDates = info.dates.sort((a, b) => a.getTime() - b.getTime());
    const { startDate, endDate } = getDateBounds(sortedDates);
    result.push({
      indexCode: code,
      indexName: info.name,
      record_count: info.dates.length,
      start_date: startDate,
      end_date: endDate,
    });
  }
  return result;
}

/** Normalize Katakana middle dot (・ U+30FB) to halfwidth (･ U+FF65) for consistent sector name lookup */
function normalizeMiddleDot(text: string): string {
  return text.replace(/・/g, '･');
}

/**
 * Sector33 name → index code mapping (e.g., "化学" → "0046")
 * Built from INDEX_MASTER_DATA to provide correct J-Quants index codes.
 * Note: sector33Code on stocks (e.g., "3200") differs from index codes (e.g., "0046").
 */
const SECTOR33_NAME_TO_INDEX_CODE = new Map(
  INDEX_MASTER_DATA.filter((idx) => idx.category === INDEX_CATEGORIES.SECTOR33).map(
    (idx) => [normalizeMiddleDot(idx.name), idx.code] as const
  )
);

/**
 * Build a map of sector code to index name from sector data
 */
async function buildSectorIndexMap(reader: DatasetReader): Promise<Map<string, string>> {
  const sectorData = await reader.getSectorData();
  const indexMap = new Map<string, string>();
  for (const d of sectorData) {
    if (!indexMap.has(d.sectorCode)) {
      indexMap.set(d.sectorCode, d.sectorName);
    }
  }
  return indexMap;
}

/**
 * Filter statement record by period type
 */
function matchesPeriodType(periodType: PeriodType, currentPeriod: string | null | undefined): boolean {
  if (periodType === 'all') return true;
  if (periodType === 'FY') return isFiscalYear(currentPeriod);
  return currentPeriod === periodType;
}

/**
 * Map dataset statement fields to FinancialDataInput for hasActualFinancialData check
 */
function toFinancialDataInput(d: { earningsPerShare: number | null; profit: number | null; equity: number | null }): {
  eps: number | null;
  netProfit: number | null;
  equity: number | null;
} {
  return {
    eps: d.earningsPerShare,
    netProfit: d.profit,
    equity: d.equity,
  };
}

/**
 * Convert raw statement data to StatementsRecord response format
 */
function toStatementRecord(d: {
  disclosedDate: Date;
  typeOfCurrentPeriod: string | null;
  typeOfDocument: string | null;
  earningsPerShare: number | null;
  profit: number | null;
  equity: number | null;
  nextYearForecastEarningsPerShare: number | null;
  bps: number | null;
  sales: number | null;
  operatingProfit: number | null;
  ordinaryProfit: number | null;
  operatingCashFlow: number | null;
  dividendFY: number | null;
  forecastEps: number | null;
  investingCashFlow: number | null;
  financingCashFlow: number | null;
  cashAndEquivalents: number | null;
  totalAssets: number | null;
  sharesOutstanding: number | null;
  treasuryShares: number | null;
}): StatementsRecord {
  return {
    disclosedDate: formatDate(d.disclosedDate),
    typeOfCurrentPeriod: d.typeOfCurrentPeriod ?? '',
    typeOfDocument: d.typeOfDocument ?? '',
    earningsPerShare: d.earningsPerShare,
    profit: d.profit,
    equity: d.equity,
    nextYearForecastEarningsPerShare: d.nextYearForecastEarningsPerShare,
    bps: d.bps,
    sales: d.sales,
    operatingProfit: d.operatingProfit,
    ordinaryProfit: d.ordinaryProfit,
    operatingCashFlow: d.operatingCashFlow,
    dividendFY: d.dividendFY,
    forecastEps: d.forecastEps,
    investingCashFlow: d.investingCashFlow,
    financingCashFlow: d.financingCashFlow,
    cashAndEquivalents: d.cashAndEquivalents,
    totalAssets: d.totalAssets,
    sharesOutstanding: d.sharesOutstanding,
    treasuryShares: d.treasuryShares,
  };
}

/**
 * Filter raw statements data by period type and actual_only, then convert to records
 */
function filterAndMapStatements(
  rawData: Parameters<typeof toStatementRecord>[0][],
  periodType: PeriodType,
  actualOnly: boolean
): StatementsRecord[] {
  return rawData
    .filter((d) => {
      if (!matchesPeriodType(periodType, d.typeOfCurrentPeriod)) return false;
      if (actualOnly && !hasActualFinancialData(toFinancialDataInput(d))) return false;
      return true;
    })
    .map(toStatementRecord);
}

/**
 * Dataset Data Service
 */
export const datasetDataService = {
  /**
   * Get stock OHLCV data with optional timeframe conversion
   *
   * Note: weekly/monthlyの場合はbt/ APIに委譲
   * 仕様: apps/bt/docs/spec-timeframe-resample.md
   */
  async getStockOHLCV(datasetName: string, stockCode: string, query: OHLCVQuery): Promise<OHLCVRecord[] | null> {
    const timeframe: Timeframe = query.timeframe ?? 'daily';

    // weekly/monthlyはbt/ APIに委譲
    if (timeframe === 'weekly' || timeframe === 'monthly') {
      return resampleOHLCVviaBt(stockCode, timeframe, query.start_date, query.end_date);
    }

    // dailyの場合はDatasetReaderから直接取得
    const reader = getReader(datasetName);
    if (!reader) return null;

    try {
      const dateRange = buildDateRange(query.start_date, query.end_date);
      const data = await reader.getStockData(stockCode, dateRange);
      return toOHLCVRecordsFromRaw(data);
    } catch (error) {
      logger.error('Failed to get stock OHLCV', { datasetName, stockCode, error });
      return null;
    } finally {
      await reader.close();
    }
  },

  /**
   * Get batch stock OHLCV data for multiple stock codes
   * Returns a record mapping stock codes to their OHLCV data
   *
   * Note: weekly/monthlyの場合はbt/ APIに委譲
   */
  async getStockOHLCVBatch(datasetName: string, query: BatchOHLCVQuery): Promise<BatchOHLCVResponse | null> {
    const codes = query.codes.split(',').map((c) => c.trim());
    const timeframe: Timeframe = query.timeframe ?? 'daily';

    // weekly/monthlyはbt/ APIに委譲（個別にリサンプル）
    if (timeframe === 'weekly' || timeframe === 'monthly') {
      const result: Record<string, OHLCVRecord[]> = {};

      for (const code of codes) {
        const btResult = await resampleOHLCVviaBt(code, timeframe, query.start_date, query.end_date);
        if (btResult !== null && btResult.length > 0) {
          result[code] = btResult;
        }
      }

      return Object.keys(result).length > 0 ? result : null;
    }

    // dailyの場合はDatasetReaderから直接取得
    const reader = getReader(datasetName);
    if (!reader) return null;

    try {
      const dateRange = buildDateRange(query.start_date, query.end_date);

      return await processBatched(codes, async (code) => {
        try {
          const data = await reader.getStockData(code, dateRange);
          if (data.length === 0) return null;
          return { key: code, value: toOHLCVRecordsFromRaw(data) };
        } catch {
          return null;
        }
      });
    } catch (error) {
      logger.error('Failed to get batch stock OHLCV', { datasetName, error });
      return null;
    } finally {
      await reader.close();
    }
  },

  /**
   * Get batch margin data for multiple stock codes
   */
  async getMarginBatch(datasetName: string, query: BatchMarginQuery): Promise<BatchMarginResponse | null> {
    const reader = getReader(datasetName);
    if (!reader) return null;

    try {
      const codes = query.codes.split(',').map((c) => c.trim());
      const dateRange = buildDateRange(query.start_date, query.end_date);

      return await processBatched(codes, async (code) => {
        try {
          const data = await reader.getMarginData(code, dateRange);
          if (data.length === 0) return null;
          return {
            key: code,
            value: data.map((d) => ({
              date: formatDate(d.date),
              longMarginVolume: d.longMarginVolume ?? 0,
              shortMarginVolume: d.shortMarginVolume ?? 0,
            })),
          };
        } catch {
          return null;
        }
      });
    } catch (error) {
      logger.error('Failed to get batch margin data', { datasetName, error });
      return null;
    } finally {
      await reader.close();
    }
  },

  /**
   * Get batch statements data for multiple stock codes
   */
  async getStatementsBatch(datasetName: string, query: BatchStatementsQuery): Promise<BatchStatementsResponse | null> {
    const reader = getReader(datasetName);
    if (!reader) return null;

    try {
      const codes = query.codes.split(',').map((c) => c.trim());
      const dateRange = buildDateRange(query.start_date, query.end_date);
      const periodType: PeriodType = query.period_type ?? 'all';
      const actualOnly = query.actual_only === 'true';

      return await processBatched(codes, async (code) => {
        try {
          const rawData = await reader.getStatementsData(code, dateRange);
          if (rawData.length === 0) return null;
          const records = filterAndMapStatements(rawData, periodType, actualOnly);
          if (records.length === 0) return null;
          return { key: code, value: records };
        } catch {
          return null;
        }
      });
    } catch (error) {
      logger.error('Failed to get batch statements data', { datasetName, error });
      return null;
    } finally {
      await reader.close();
    }
  },

  /**
   * Get stock list
   */
  async getStockList(datasetName: string, query: StockListQuery): Promise<StockListItem[] | null> {
    const reader = getReader(datasetName);
    if (!reader) return null;

    try {
      const stocks = await reader.getStockList();
      const minRecords = query.min_records ?? 100;

      let result: StockListItem[];
      if (query.detail === 'true') {
        result = await buildDetailedStockList(reader, stocks, minRecords);
      } else {
        result = await buildSimpleStockList(reader, stocks, minRecords, query.limit);
      }

      return result;
    } catch (error) {
      logger.error('Failed to get stock list', { datasetName, error });
      return null;
    } finally {
      await reader.close();
    }
  },

  /**
   * Get TOPIX data
   */
  async getTopix(datasetName: string, query: DateRangeQuery): Promise<OHLCRecord[] | null> {
    const reader = getReader(datasetName);
    if (!reader) return null;

    try {
      const dateRange = buildDateRange(query.start_date, query.end_date);
      const data = await reader.getTopixData(dateRange);

      return data.map((d) => ({
        date: formatDate(d.date),
        open: d.open,
        high: d.high,
        low: d.low,
        close: d.close,
      }));
    } catch (error) {
      logger.error('Failed to get TOPIX data', { datasetName, error });
      return null;
    } finally {
      await reader.close();
    }
  },

  /**
   * Get index data
   */
  async getIndex(datasetName: string, indexCode: string, query: DateRangeQuery): Promise<OHLCRecord[] | null> {
    const reader = getReader(datasetName);
    if (!reader) return null;

    try {
      const dateRange = buildDateRange(query.start_date, query.end_date);
      const data = await reader.getSectorData(indexCode, dateRange);

      return data.map((d) => ({
        date: formatDate(d.date),
        open: d.open,
        high: d.high,
        low: d.low,
        close: d.close,
      }));
    } catch (error) {
      logger.error('Failed to get index data', { datasetName, indexCode, error });
      return null;
    } finally {
      await reader.close();
    }
  },

  /**
   * Get index list
   */
  async getIndexList(datasetName: string, query: IndexListQuery): Promise<IndexListItem[] | null> {
    const reader = getReader(datasetName);
    if (!reader) return null;

    try {
      const sectorData = await reader.getSectorData();
      const indexMap = buildIndexMap(sectorData);
      const filterCodes = query.codes?.split(',').map((c) => c.trim());
      const minRecords = query.min_records ?? 100;

      return buildIndexListItems(indexMap, filterCodes, minRecords);
    } catch (error) {
      logger.error('Failed to get index list', { datasetName, error });
      return null;
    } finally {
      await reader.close();
    }
  },

  /**
   * Get margin data
   */
  async getMargin(datasetName: string, stockCode: string, query: DateRangeQuery): Promise<MarginRecord[] | null> {
    const reader = getReader(datasetName);
    if (!reader) return null;

    try {
      const dateRange = buildDateRange(query.start_date, query.end_date);
      const data = await reader.getMarginData(stockCode, dateRange);

      return data.map((d) => ({
        date: formatDate(d.date),
        longMarginVolume: d.longMarginVolume ?? 0,
        shortMarginVolume: d.shortMarginVolume ?? 0,
      }));
    } catch (error) {
      logger.error('Failed to get margin data', { datasetName, stockCode, error });
      return null;
    } finally {
      await reader.close();
    }
  },

  /**
   * Get margin list (stocks with margin data)
   */
  async getMarginList(datasetName: string, query: MarginListQuery): Promise<MarginListItem[] | null> {
    const reader = getReader(datasetName);
    if (!reader) return null;

    try {
      const stocks = await reader.getStockList();
      const filterCodes = query.codes?.split(',').map((c) => c.trim());

      const result: MarginListItem[] = [];
      for (const stock of stocks) {
        if (filterCodes && !filterCodes.includes(stock.code)) continue;

        const marginData = await reader.getMarginData(stock.code);
        if (marginData.length >= (query.min_records ?? 10)) {
          const sortedDates = marginData.map((d) => d.date).sort((a, b) => a.getTime() - b.getTime());
          const { startDate, endDate } = getDateBounds(sortedDates);
          const avgLong = marginData.reduce((sum, d) => sum + (d.longMarginVolume ?? 0), 0) / marginData.length;
          const avgShort = marginData.reduce((sum, d) => sum + (d.shortMarginVolume ?? 0), 0) / marginData.length;

          result.push({
            stockCode: stock.code,
            record_count: marginData.length,
            start_date: startDate,
            end_date: endDate,
            avg_long_margin: avgLong,
            avg_short_margin: avgShort,
          });
        }
      }

      return result;
    } catch (error) {
      logger.error('Failed to get margin list', { datasetName, error });
      return null;
    } finally {
      await reader.close();
    }
  },

  /**
   * Get statements data with optional period type and actual_only filtering
   */
  async getStatements(
    datasetName: string,
    stockCode: string,
    query: StatementsQuery
  ): Promise<StatementsRecord[] | null> {
    const reader = getReader(datasetName);
    if (!reader) return null;

    try {
      const dateRange = buildDateRange(query.start_date, query.end_date);
      const rawData = await reader.getStatementsData(stockCode, dateRange);
      const periodType: PeriodType = query.period_type ?? 'all';
      const actualOnly = query.actual_only === 'true';
      return filterAndMapStatements(rawData, periodType, actualOnly);
    } catch (error) {
      logger.error('Failed to get statements', { datasetName, stockCode, error });
      return null;
    } finally {
      await reader.close();
    }
  },

  /**
   * Get sector mapping
   */
  async getSectorMapping(datasetName: string): Promise<SectorMappingRecord[] | null> {
    const reader = getReader(datasetName);
    if (!reader) return null;

    try {
      const stocks = await reader.getStockList();

      // Extract unique sector mappings
      const sectorMap = new Map<string, { sectorName: string }>();
      for (const stock of stocks) {
        if (!sectorMap.has(stock.sector33Code)) {
          sectorMap.set(stock.sector33Code, { sectorName: stock.sector33Name ?? '' });
        }
      }

      const indexMap = await buildSectorIndexMap(reader);

      const result: SectorMappingRecord[] = [];
      for (const [code, info] of sectorMap) {
        const indexCode = SECTOR33_NAME_TO_INDEX_CODE.get(normalizeMiddleDot(info.sectorName));
        if (!indexCode) {
          logger.warn(`Sector name "${info.sectorName}" not found in INDEX_MASTER_DATA, falling back to sector33Code`);
        }
        result.push({
          sector_code: code,
          sector_name: info.sectorName,
          index_code: indexCode ?? code,
          index_name: indexMap.get(code) ?? info.sectorName,
        });
      }

      return result;
    } catch (error) {
      logger.error('Failed to get sector mapping', { datasetName, error });
      return null;
    } finally {
      await reader.close();
    }
  },

  /**
   * Get stock to sector name mapping for all stocks
   */
  async getStockSectorMapping(datasetName: string): Promise<StockSectorMappingItem[] | null> {
    const reader = getReader(datasetName);
    if (!reader) return null;

    try {
      const stocks = await reader.getStockList();
      return stocks
        .filter((stock) => stock.sector33Name?.trim())
        .map((stock) => ({
          code: stock.code,
          sector33Name: stock.sector33Name,
        }));
    } catch (error) {
      logger.error('Failed to get stock sector mapping', { datasetName, error });
      return null;
    } finally {
      await reader.close();
    }
  },

  /**
   * Get stock codes belonging to a specific sector
   */
  async getSectorStocks(datasetName: string, sectorName: string): Promise<string[] | null> {
    const reader = getReader(datasetName);
    if (!reader) return null;

    try {
      const stocks = await reader.getStockList();
      return stocks.filter((stock) => stock.sector33Name === sectorName).map((stock) => stock.code);
    } catch (error) {
      logger.error('Failed to get sector stocks', { datasetName, sectorName, error });
      return null;
    } finally {
      await reader.close();
    }
  },

  /**
   * Get all sectors with stock count
   */
  async getSectorsWithCount(datasetName: string): Promise<SectorWithCountRecord[] | null> {
    const reader = getReader(datasetName);
    if (!reader) return null;

    try {
      const stocks = await reader.getStockList();

      // Count stocks per sector
      const sectorCountMap = new Map<string, { sectorName: string; count: number }>();
      for (const stock of stocks) {
        if (!stock.sector33Code) continue;
        const existing = sectorCountMap.get(stock.sector33Code);
        if (existing) {
          existing.count++;
        } else {
          sectorCountMap.set(stock.sector33Code, { sectorName: stock.sector33Name ?? '', count: 1 });
        }
      }

      const indexMap = await buildSectorIndexMap(reader);

      const result: SectorWithCountRecord[] = [];
      for (const [code, info] of sectorCountMap) {
        result.push({
          sector_code: code,
          sector_name: info.sectorName,
          index_code: SECTOR33_NAME_TO_INDEX_CODE.get(normalizeMiddleDot(info.sectorName)) ?? code,
          index_name: indexMap.get(code) ?? info.sectorName,
          stock_count: info.count,
        });
      }

      return result;
    } catch (error) {
      logger.error('Failed to get sectors with count', { datasetName, error });
      return null;
    } finally {
      await reader.close();
    }
  },
};
