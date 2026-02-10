/**
 * Drizzle ORM Database Module
 *
 * Provides type-safe database operations with automatic stock code normalization.
 */

// ===== Column Types =====
export {
  expandStockCode,
  isValidStockCode,
  normalizeStockCode,
  stockCode,
} from './columns';

// ===== Database Implementations =====
export {
  DATASET_METADATA_KEYS,
  DrizzleDatasetDatabase,
  DrizzleDatasetDatabase as DatabaseV2,
} from './drizzle-dataset-database';

export type { AdjustmentEvent } from './drizzle-market-database';
export {
  DrizzleMarketDatabase,
  DrizzleMarketDatabase as MarketDatabase,
  METADATA_KEYS,
} from './drizzle-market-database';

export type { RankingItem, StockSearchResult } from './drizzle-market-reader';
export { DrizzleMarketDataReader, DrizzleMarketDataReader as MarketDataReader } from './drizzle-market-reader';

export { DrizzlePortfolioDatabase, DrizzlePortfolioDatabase as PortfolioDatabase } from './drizzle-portfolio-database';

export { DrizzleWatchlistDatabase, DrizzleWatchlistDatabase as WatchlistDatabase } from './drizzle-watchlist-database';

// ===== Schema Definitions =====
// Dataset schema
export {
  DATASET_SCHEMA_VERSION,
  type DatasetDailyQuote,
  type DatasetIndex,
  type DatasetInfoRecord,
  type DatasetMarginData,
  type DatasetStatement,
  type DatasetStock,
  type DatasetTopix,
  datasetDailyQuotes,
  datasetIndices,
  datasetInfo,
  datasetMarginData,
  datasetStatements,
  datasetStocks,
  datasetTopix,
  type NewDatasetDailyQuote,
  type NewDatasetIndex,
  type NewDatasetInfoRecord,
  type NewDatasetMarginData,
  type NewDatasetStatement,
  type NewDatasetStock,
  type NewDatasetTopix,
} from './schema/dataset-schema';

// Market schema
export {
  INDEX_CATEGORIES,
  type IndexCategory,
  type IndexMasterInsert,
  type IndexMasterRow,
  type IndicesDataInsert,
  type IndicesDataRow,
  indexMaster,
  indicesData,
  MARKET_SCHEMA_VERSION,
  type StockDataInsert,
  type StockDataRow,
  type StockInsert,
  type StockRow,
  type SyncMetadataInsert,
  type SyncMetadataRow,
  stockData,
  stocks,
  syncMetadata,
  type TopixDataInsert,
  type TopixDataRow,
  topixData,
} from './schema/market-schema';

// Portfolio schema
export {
  PORTFOLIO_SCHEMA_VERSION,
  type PortfolioInsert,
  type PortfolioItemInsert,
  type PortfolioItemRow,
  type PortfolioMetadataInsert,
  type PortfolioMetadataRow,
  type PortfolioRow,
  portfolioItems,
  portfolioMetadata,
  portfolios,
} from './schema/portfolio-schema';

// Watchlist schema
export {
  WATCHLIST_SCHEMA_VERSION,
  type WatchlistInsert,
  type WatchlistItemInsert,
  type WatchlistItemRow,
  type WatchlistRow,
  watchlistItems,
  watchlists,
} from './schema/watchlist-schema';
