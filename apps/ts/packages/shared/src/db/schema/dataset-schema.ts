/**
 * Drizzle Schema for Dataset Database
 *
 * Defines type-safe schema for dataset databases with automatic
 * stock code normalization (5-digit JQuants -> 4-digit).
 *
 * Uses unified snake_case naming convention matching market.db
 */

/**
 * Schema version for dataset database
 */
export const DATASET_SCHEMA_VERSION = '2.0.0';

// Re-export common tables with dataset aliases for backward compatibility
export {
  type IndicesDataInsert as NewDatasetIndex,
  type IndicesDataRow as DatasetIndex,
  indicesData as datasetIndices,
  type StockDataInsert as NewDatasetDailyQuote,
  type StockDataRow as DatasetDailyQuote,
  type StockInsert as NewDatasetStock,
  type StockRow as DatasetStock,
  stockData as datasetDailyQuotes,
  stocks as datasetStocks,
  type TopixDataInsert as NewDatasetTopix,
  type TopixDataRow as DatasetTopix,
  topixData as datasetTopix,
} from './common';

// Re-export dataset-specific tables
export {
  type DatasetInfoInsert as NewDatasetInfoRecord,
  type DatasetInfoRow as DatasetInfoRecord,
  datasetInfo,
  type MarginDataInsert as NewDatasetMarginData,
  type MarginDataRow as DatasetMarginData,
  marginData as datasetMarginData,
  type StatementsInsert as NewDatasetStatement,
  type StatementsRow as DatasetStatement,
  statements as datasetStatements,
} from './dataset';
