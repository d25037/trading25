/**
 * Drizzle Schema Exports
 */

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
} from './dataset-schema';

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
} from './market-schema';

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
} from './portfolio-schema';
