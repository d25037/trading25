/**
 * Trading25 Shared Package - Main Entry Point
 *
 * This module provides a carefully curated public API.
 * For specialized functionality, use subpath imports:
 * - @trading25/shared/ta
 * - @trading25/shared/dataset
 * - @trading25/shared/portfolio
 */

// ===== CLIENT EXPORTS =====
// Direct J-Quants clients are intentionally not re-exported.
// Use bt FastAPI endpoints (`/api/jquants/*`) from web/cli/shared modules.
// ===== CONFIGURATION EXPORTS =====
export type {
  AppConfig,
  DatabaseConfig,
  DatasetConfig as SystemDatasetConfig,
  RateLimiterConfig,
} from './config';
export { getConfig, resetConfig, setConfig } from './config';
// ===== DATASET EXPORTS =====
// Types
export type {
  ApiError,
  BuildResult,
  DatabaseError,
  DatasetConfig,
  DatasetError,
  DatasetStats,
  DateRange,
  DebugConfig,
  MarginData,
  MarketType,
  ProgressCallback,
  ProgressInfo,
  SectorData,
  StatementsData,
  StockData,
  StockInfo,
  TopixData,
} from './dataset';
// Classes and Functions
export {
  ApiClient,
  BatchExecutor,
  ConsoleProgressFormatter,
  chunkArray,
  createBatchExecutor,
  createConfig,
  createConsoleProgressCallback,
  createCustomConfig,
  createCustomDateRange,
  createDateRange,
  createDebugConfig,
  createErrorSummary,
  createForDateRangeConfig,
  createSilentProgressCallback,
  DEFAULT_DEBUG_CONFIG,
  debounce,
  filterStocksByMarkets,
  filterStocksByScaleCategories,
  filterStocksBySectors,
  formatDateForApi,
  formatFileSize,
  generateUniqueFilename,
  getDatasetDateRange,
  getDateRangeStrings,
  getDaysInRange,
  getMarketCode,
  getMarketCodes,
  getMarketType,
  getUniqueValues,
  groupStocksByMarket,
  groupStocksBySector,
  isDateInRange,
  isDefined,
  isNonEmptyArray,
  isNonEmptyString,
  isValidDateRange,
  isValidSectorCode,
  isValidStockCode,
  MarginDataSchema,
  MarketTypeSchema,
  MultiStageProgressTracker,
  measureTime,
  ProgressTracker,
  presets,
  removeDuplicatesBy,
  resolveDatasetConcurrency,
  SectorDataSchema,
  StatementsDataSchema,
  StockDataSchema,
  StockInfoSchema,
  type StreamConfig,
  StreamingFetchers,
  StreamingUtils,
  type StreamResult,
  safeJsonStringify,
  safeValidateStockDataArray,
  safeValidateStockInfo,
  sanitizeFilePath,
  sleep,
  TopixDataSchema,
  validateConfig,
  validateDataArray,
  validateDatasetConfig,
  validateDatasetConsistency,
  validateDateRange,
  validateFilePath,
  validateMarginData,
  validateMarketType,
  validateSectorCode,
  validateSectorData,
  validateStatementsData,
  validateStockCode,
  validateStockData,
  validateStockDataArray,
  validateStockInfo,
  validateStockInfoArray,
  validateTopixData,
} from './dataset';
// ===== ERROR EXPORTS =====
export {
  BadRequestError,
  ConflictError,
  getErrorMessage,
  InternalError,
  isTrading25Error,
  NotFoundError,
  Trading25Error,
} from './errors';

// ===== PORTFOLIO EXPORTS =====
export type {
  CreatePortfolioInput,
  CreatePortfolioItemInput,
  DeleteResponse,
  ListPortfoliosResponse,
  Portfolio,
  PortfolioItem,
  PortfolioItemResponse,
  PortfolioResponse,
  PortfolioSummary,
  PortfolioSummaryResponse,
  PortfolioWithItems,
  PortfolioWithItemsResponse,
  UpdatePortfolioInput,
  UpdatePortfolioItemInput,
} from './portfolio';
export {
  DuplicatePortfolioNameError,
  DuplicateStockError,
  InvalidStockCodeError,
  PortfolioError,
  PortfolioItemNotFoundError,
  PortfolioNameNotFoundError,
  PortfolioNotFoundError,
  StockNotFoundInPortfolioError,
  ValidationError,
} from './portfolio';
// ===== SERVICE EXPORTS =====
export { type AuthCredentials, AuthService, type AuthStatus } from './services/auth-service';
// ===== TECHNICAL ANALYSIS EXPORTS =====
// Phase 4.3: Timeframe変換、Relative OHLCはbt/ API移行完了。Utilitiesのみ残存。
export { cleanNaNValues } from './ta';
// ===== API RESPONSE TYPE EXPORTS =====
export type {
  AdjustmentEvent as ApiAdjustmentEvent,
  CancelDatasetJobResponse,
  CancelJobResponse,
  CreateSyncJobResponse,
  DatasetCreateJobResponse,
  DatasetCreateRequest,
  DatasetDeleteResponse,
  DatasetInfoResponse,
  DatasetJobProgress,
  DatasetJobResponse,
  DatasetListItem,
  DatasetListResponse,
  FundamentalRankingItem,
  FundamentalRankingSource,
  FundamentalRankings,
  IndexDataPoint,
  IndexDataResponse,
  IndexItem,
  IndicesListResponse,
  IntegrityIssue,
  JobProgress,
  JobStatus as SyncJobStatus,
  MarketFundamentalRankingResponse,
  MarketRankingResponse,
  MarketScreeningResponse,
  MarketValidationResponse,
  MatchedStrategyItem,
  PresetInfo,
  RankingItem as ApiRankingItem,
  Rankings,
  RankingType,
  ScreeningJobRequest,
  ScreeningJobResponse,
  ScreeningResultItem,
  ScreeningSortBy,
  ScreeningSummary,
  SortOrder,
  SyncJobResponse,
  SyncJobResult,
  SyncMode,
} from './types/api-response-types';
export { DATASET_PRESETS } from './types/api-response-types';

// ===== TYPE EXPORTS (API) =====
export type {
  ApiDailyValuationDataPoint,
  ApiExcludedStock,
  ApiFactorRegressionResponse,
  ApiFundamentalDataPoint,
  ApiFundamentalsResponse,
  ApiIndexMatch,
  ApiMarginFlowPressureData,
  ApiMarginLongPressureData,
  ApiMarginPressureIndicatorsResponse,
  ApiMarginTurnoverDaysData,
  ApiMarginVolumeRatioData,
  ApiMarginVolumeRatioResponse,
  ApiPortfolioFactorRegressionResponse,
  ApiPortfolioWeight,
  ApiStockDataPoint,
  ApiStockDataResponse,
  ApiTopixDataPoint,
  ApiTopixDataResponse,
  IndicatorData,
  IndicatorValue,
  MACDIndicatorData,
  PPOIndicatorData,
} from './types/api-types';

// ===== TYPE EXPORTS (JQuants) =====
export type {
  JQuantsConfig,
  JQuantsDailyQuote,
  JQuantsDailyQuotesParams,
  JQuantsDailyQuotesResponse,
  JQuantsErrorResponse,
  JQuantsIndex,
  JQuantsIndicesParams,
  JQuantsIndicesResponse,
  JQuantsListedInfo,
  JQuantsListedInfoParams,
  JQuantsListedInfoResponse,
  JQuantsStatement,
  JQuantsStatementsParams,
  JQuantsStatementsResponse,
  JQuantsTOPIX,
  JQuantsTOPIXParams,
  JQuantsTOPIXResponse,
  JQuantsTradingCalendar,
  JQuantsTradingCalendarParams,
  JQuantsTradingCalendarResponse,
  JQuantsWeeklyMarginInterest,
  JQuantsWeeklyMarginInterestParams,
  JQuantsWeeklyMarginInterestResponse,
} from './types/jquants';
// ===== UTILITY EXPORTS =====
export { BrowserTokenStorage } from './utils/browser-token-storage';
export {
  getDatasetPath,
  getDatasetV2Path,
  getMarketDbPath,
  getPortfolioDbPath,
  normalizeDatasetPath,
  resolveDatasetPath,
} from './utils/dataset-paths';
export { dateRangeToISO, toISODateString, toISODateStringOrDefault, toISODateStringOrNull } from './utils/date-helpers';
export { EnvManager, type EnvTokens } from './utils/env-manager';
export { FileTokenStorage } from './utils/file-token-storage';
export { findProjectRoot } from './utils/find-project-root';
export { ConsoleLogger, createDefaultLogger, type Logger, SilentLogger } from './utils/logger';
export type { ILogger, ILoggerFactory, LogContext } from './utils/logger-interface';
export { SecureEnvManager } from './utils/secure-env-manager';
export { TokenManager } from './utils/token-manager';
export type { TokenData, TokenStorage, TokenStorageOptions } from './utils/token-storage';
// ===== WATCHLIST EXPORTS =====
export type {
  CreateWatchlistInput,
  CreateWatchlistItemInput,
  ListWatchlistsResponse,
  UpdateWatchlistInput,
  Watchlist,
  WatchlistDeleteResponse,
  WatchlistItem,
  WatchlistItemResponse,
  WatchlistPricesResponse,
  WatchlistResponse,
  WatchlistStockPrice,
  WatchlistSummary,
  WatchlistSummaryResponse,
  WatchlistWithItems,
  WatchlistWithItemsResponse,
} from './watchlist';
export {
  DuplicateWatchlistNameError,
  DuplicateWatchlistStockError,
  StockNotFoundInWatchlistError,
  WatchlistError,
  WatchlistItemNotFoundError,
  WatchlistNameNotFoundError,
  WatchlistNotFoundError,
} from './watchlist';
