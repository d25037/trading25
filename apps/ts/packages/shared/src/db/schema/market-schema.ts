/**
 * Market Database Drizzle Schema
 *
 * This schema defines the structure for market-wide data:
 * - stocks: Stock information (company name, sector, market)
 * - stock_data: Daily OHLCV data for each stock
 * - topix_data: TOPIX index daily data (used as trading calendar)
 * - indices_data: All index daily OHLC data
 * - sync_metadata: Key-value storage for sync state
 * - index_master: Index definitions and metadata
 */

/**
 * Schema version for market database
 */
export const MARKET_SCHEMA_VERSION = '3.0.0';

// Re-export common tables
export {
  type IndicesDataInsert,
  type IndicesDataRow,
  indicesData,
  type StockDataInsert,
  type StockDataRow,
  type StockInsert,
  type StockRow,
  stockData,
  stocks,
  type TopixDataInsert,
  type TopixDataRow,
  topixData,
} from './common';

// Re-export market-specific tables
export {
  INDEX_CATEGORIES,
  type IndexCategory,
  type IndexMasterInsert,
  type IndexMasterRow,
  indexMaster,
  type SyncMetadataInsert,
  type SyncMetadataRow,
  syncMetadata,
} from './market';
