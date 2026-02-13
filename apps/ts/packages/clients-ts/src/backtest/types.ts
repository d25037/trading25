/**
 * Backtest API Types
 *
 * trading25-bt FastAPI サーバーとの通信用型定義
 */

export type JobStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';

export interface BacktestRequest {
  strategy_name: string;
  strategy_config_override?: Record<string, unknown>;
}

export interface BacktestResultSummary {
  total_return: number;
  sharpe_ratio: number;
  calmar_ratio: number;
  max_drawdown: number;
  win_rate: number;
  trade_count: number;
  html_path: string | null;
}

export interface BacktestJobResponse {
  job_id: string;
  status: JobStatus;
  progress: number | null;
  message: string | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  error: string | null;
  result: BacktestResultSummary | null;
}

export interface BacktestResultResponse {
  job_id: string;
  strategy_name: string;
  dataset_name: string;
  summary: BacktestResultSummary;
  execution_time: number;
  html_content: string | null;
  created_at: string;
}

export interface SignalAttributionRequest {
  strategy_name: string;
  strategy_config_override?: Record<string, unknown>;
  shapley_top_n?: number;
  shapley_permutations?: number;
  random_seed?: number | null;
}

export interface SignalAttributionMetrics {
  total_return: number;
  sharpe_ratio: number;
}

export interface SignalAttributionLooResult {
  status: 'ok' | 'error';
  variant_metrics: SignalAttributionMetrics | null;
  delta_total_return: number | null;
  delta_sharpe_ratio: number | null;
  error: string | null;
}

export interface SignalAttributionShapleyResult {
  status: 'ok' | 'error';
  total_return: number | null;
  sharpe_ratio: number | null;
  method: string;
  sample_size: number | null;
  error: string | null;
}

export interface SignalAttributionSignalResult {
  signal_id: string;
  scope: 'entry' | 'exit';
  param_key: string;
  signal_name: string;
  loo: SignalAttributionLooResult;
  shapley: SignalAttributionShapleyResult | null;
}

export interface SignalAttributionTopNScore {
  signal_id: string;
  score: number;
}

export interface SignalAttributionTopNSelection {
  top_n_requested: number;
  top_n_effective: number;
  selected_signal_ids: string[];
  scores: SignalAttributionTopNScore[];
}

export interface SignalAttributionTiming {
  total_seconds: number;
  baseline_seconds: number;
  loo_seconds: number;
  shapley_seconds: number;
}

export interface SignalAttributionShapleyMeta {
  method: string | null;
  sample_size: number | null;
  error: string | null;
  evaluations: number | null;
}

export interface SignalAttributionResult {
  baseline_metrics: SignalAttributionMetrics;
  signals: SignalAttributionSignalResult[];
  top_n_selection: SignalAttributionTopNSelection;
  timing: SignalAttributionTiming;
  shapley: SignalAttributionShapleyMeta;
}

export interface SignalAttributionJobResponse {
  job_id: string;
  status: JobStatus;
  progress: number | null;
  message: string | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  error: string | null;
  result_data: SignalAttributionResult | null;
}

export interface SignalAttributionResultResponse {
  job_id: string;
  strategy_name: string;
  result: SignalAttributionResult;
  created_at: string;
}

export interface AttributionArtifactInfo {
  strategy_name: string;
  filename: string;
  created_at: string;
  size_bytes: number;
  job_id: string | null;
}

export interface AttributionArtifactListResponse {
  files: AttributionArtifactInfo[];
  total: number;
}

export interface AttributionArtifactContentResponse {
  strategy_name: string;
  filename: string;
  artifact: Record<string, unknown>;
}

export interface StrategyMetadata {
  name: string;
  category: string;
  display_name: string | null;
  description: string | null;
  last_modified: string | null;
}

export interface StrategyListResponse {
  strategies: StrategyMetadata[];
  total: number;
}

export interface StrategyDetailResponse {
  name: string;
  category: string;
  display_name: string | null;
  description: string | null;
  config: Record<string, unknown>;
  execution_info: Record<string, unknown>;
}

export interface StrategyValidationRequest {
  config: Record<string, unknown>;
}

export interface StrategyValidationResponse {
  valid: boolean;
  errors: string[];
  warnings: string[];
}

export interface HealthResponse {
  status: string;
  service: string;
  version: string;
}

export interface BacktestClientConfig {
  baseUrl: string;
  timeout?: number;
}

// ============================================
// Strategy CRUD Types
// ============================================

export interface StrategyUpdateRequest {
  config: Record<string, unknown>;
}

export interface StrategyUpdateResponse {
  success: boolean;
  strategy_name: string;
  path: string;
}

export interface StrategyDeleteResponse {
  success: boolean;
  strategy_name: string;
}

export interface StrategyDuplicateRequest {
  new_name: string;
}

export interface StrategyDuplicateResponse {
  success: boolean;
  new_strategy_name: string;
  path: string;
}

export interface StrategyRenameRequest {
  new_name: string;
}

export interface StrategyRenameResponse {
  success: boolean;
  old_name: string;
  new_name: string;
  new_path: string;
}

// ============================================
// HTML File Browser Types
// ============================================

export interface HtmlFileInfo {
  strategy_name: string;
  filename: string;
  dataset_name: string;
  created_at: string;
  size_bytes: number;
}

export interface HtmlFileListResponse {
  files: HtmlFileInfo[];
  total: number;
}

export interface HtmlFileMetrics {
  total_return: number | null;
  max_drawdown: number | null;
  sharpe_ratio: number | null;
  sortino_ratio: number | null;
  calmar_ratio: number | null;
  win_rate: number | null;
  profit_factor: number | null;
  total_trades: number | null;
}

export interface HtmlFileContentResponse {
  strategy_name: string;
  filename: string;
  html_content: string;
  metrics: HtmlFileMetrics | null;
}

export interface HtmlFileRenameRequest {
  new_filename: string;
}

export interface HtmlFileRenameResponse {
  success: boolean;
  strategy_name: string;
  old_filename: string;
  new_filename: string;
}

export interface HtmlFileDeleteResponse {
  success: boolean;
  strategy_name: string;
  filename: string;
}

// ============================================
// Optimization Types
// ============================================

export interface OptimizationRequest {
  strategy_name: string;
}

export interface OptimizationJobResponse {
  job_id: string;
  status: JobStatus;
  progress: number | null;
  message: string | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  error: string | null;
  best_score: number | null;
  total_combinations: number | null;
  notebook_path: string | null;
}

export interface OptimizationGridConfig {
  strategy_name: string;
  content: string;
  param_count: number;
  combinations: number;
}

export interface OptimizationGridListResponse {
  configs: OptimizationGridConfig[];
  total: number;
}

export interface OptimizationGridSaveRequest {
  content: string;
}

export interface OptimizationGridSaveResponse {
  success: boolean;
  strategy_name: string;
  param_count: number;
  combinations: number;
}

export interface OptimizationHtmlFileInfo {
  strategy_name: string;
  filename: string;
  dataset_name: string;
  created_at: string;
  size_bytes: number;
}

export interface OptimizationHtmlFileListResponse {
  files: OptimizationHtmlFileInfo[];
  total: number;
}

export interface OptimizationHtmlFileContentResponse {
  strategy_name: string;
  filename: string;
  html_content: string;
}

// ============================================
// Signal Reference Types
// ============================================

export interface FieldConstraints {
  gt?: number;
  ge?: number;
  lt?: number;
  le?: number;
}

export interface SignalFieldDefinition {
  name: string;
  type: 'boolean' | 'number' | 'string' | 'select';
  description: string;
  default?: unknown;
  options?: string[] | null;
  constraints?: FieldConstraints;
}

export interface SignalDefinition {
  key: string;
  name: string;
  category: string;
  description: string;
  usage_hint: string;
  fields: SignalFieldDefinition[];
  yaml_snippet: string;
  exit_disabled: boolean;
  data_requirements: string[];
}

export interface SignalCategory {
  key: string;
  label: string;
}

export interface SignalReferenceResponse {
  signals: SignalDefinition[];
  categories: SignalCategory[];
  total: number;
}

// ============================================
// Default Config Types
// ============================================

export interface DefaultConfigResponse {
  content: string;
}

export interface DefaultConfigUpdateRequest {
  content: string;
}

export interface DefaultConfigUpdateResponse {
  success: boolean;
}

// ============================================
// Lab Types
// TODO: bt-openapi sync 後に自動生成型へ置換
// ============================================

export type LabType = 'generate' | 'evolve' | 'optimize' | 'improve';
export type LabSignalCategory =
  | 'breakout'
  | 'trend'
  | 'oscillator'
  | 'volatility'
  | 'volume'
  | 'macro'
  | 'fundamental'
  | 'sector';

// Request types

export interface LabGenerateRequest {
  count?: number;
  top?: number;
  seed?: number;
  save?: boolean;
  direction?: 'longonly' | 'shortonly' | 'both';
  timeframe?: string;
  dataset?: string;
  entry_filter_only?: boolean;
  allowed_categories?: LabSignalCategory[];
}

export interface LabEvolveRequest {
  strategy_name: string;
  generations?: number;
  population?: number;
  save?: boolean;
}

export interface LabOptimizeRequest {
  strategy_name: string;
  trials?: number;
  sampler?: string;
  save?: boolean;
  scoring_weights?: Record<string, number>;
}

export interface LabImproveRequest {
  strategy_name: string;
  auto_apply?: boolean;
  entry_filter_only?: boolean;
  allowed_categories?: LabSignalCategory[];
}

// Result item types

export interface GenerateResultItem {
  strategy_id: string;
  score: number;
  sharpe_ratio: number;
  calmar_ratio: number;
  total_return: number;
  max_drawdown: number;
  win_rate: number;
  trade_count: number;
  entry_signals: string[];
  exit_signals: string[];
}

export interface EvolutionHistoryItem {
  generation: number;
  best_score: number;
  avg_score: number;
  worst_score: number;
}

export interface OptimizeTrialItem {
  trial: number;
  score: number;
  params: Record<string, unknown>;
}

export interface ImprovementItem {
  improvement_type: string;
  target: string;
  signal_name: string;
  changes: Record<string, unknown>;
  reason: string;
  expected_impact: string;
}

// Result types (discriminated union)

export interface LabGenerateResult {
  lab_type: 'generate';
  results: GenerateResultItem[];
  total_generated: number;
  saved_strategy_path?: string;
}

export interface LabEvolveResult {
  lab_type: 'evolve';
  best_strategy_id: string;
  best_score: number;
  history: EvolutionHistoryItem[];
  saved_strategy_path?: string;
  saved_history_path?: string;
}

export interface LabOptimizeResult {
  lab_type: 'optimize';
  best_score: number;
  best_params: Record<string, unknown>;
  total_trials: number;
  history: OptimizeTrialItem[];
  saved_strategy_path?: string;
  saved_history_path?: string;
}

export interface LabImproveResult {
  lab_type: 'improve';
  strategy_name: string;
  max_drawdown: number;
  max_drawdown_duration_days: number;
  suggested_improvements: ImprovementItem[];
  improvements: ImprovementItem[];
  saved_strategy_path?: string;
}

export type LabResultData = LabGenerateResult | LabEvolveResult | LabOptimizeResult | LabImproveResult;

// Response type

export interface LabJobResponse {
  job_id: string;
  lab_type?: LabType;
  strategy_name?: string;
  status: JobStatus;
  progress?: number;
  message?: string;
  created_at: string;
  started_at?: string;
  completed_at?: string;
  error?: string;
  result_data?: LabResultData;
}

// SSE Event type

export interface LabSSEEvent {
  job_id: string;
  status: JobStatus;
  progress?: number;
  message?: string;
  data?: Record<string, unknown>;
}

// ============================================
// Fundamentals Types
// ============================================

export type FundamentalsPeriodType = 'all' | 'FY' | '1Q' | '2Q' | '3Q';

export interface FundamentalsComputeRequest {
  symbol: string;
  from_date?: string;
  to_date?: string;
  period_type?: FundamentalsPeriodType;
  prefer_consolidated?: boolean;
}

export interface FundamentalDataPoint {
  date: string;
  disclosedDate: string;
  periodType: string;
  isConsolidated: boolean;
  accountingStandard: string | null;
  // Core metrics
  roe: number | null;
  eps: number | null;
  dilutedEps: number | null;
  bps: number | null;
  adjustedEps: number | null;
  adjustedForecastEps: number | null;
  adjustedBps: number | null;
  per: number | null;
  pbr: number | null;
  // Profitability metrics
  roa: number | null;
  operatingMargin: number | null;
  netMargin: number | null;
  // Financial data (millions of JPY)
  stockPrice: number | null;
  netProfit: number | null;
  equity: number | null;
  totalAssets: number | null;
  netSales: number | null;
  operatingProfit: number | null;
  // Cash flow data (millions of JPY)
  cashFlowOperating: number | null;
  cashFlowInvesting: number | null;
  cashFlowFinancing: number | null;
  cashAndEquivalents: number | null;
  // FCF metrics
  fcf: number | null;
  fcfYield: number | null;
  fcfMargin: number | null;
  // Forecast EPS
  forecastEps: number | null;
  forecastEpsChangeRate: number | null;
  // Revised forecast (from latest Q)
  revisedForecastEps?: number | null;
  revisedForecastSource?: string | null;
  // Previous period CF data
  prevCashFlowOperating: number | null;
  prevCashFlowInvesting: number | null;
  prevCashFlowFinancing: number | null;
  prevCashAndEquivalents: number | null;
}

export interface DailyValuationDataPoint {
  date: string;
  close: number;
  per: number | null;
  pbr: number | null;
  marketCap: number | null;
}

export interface FundamentalsComputeResponse {
  symbol: string;
  companyName?: string;
  data: FundamentalDataPoint[];
  latestMetrics?: FundamentalDataPoint;
  dailyValuation?: DailyValuationDataPoint[];
  lastUpdated: string;
}

// ============================================
// OHLCV Resample Types
// ============================================

export type Timeframe = 'daily' | 'weekly' | 'monthly';
export type HandleZeroDivision = 'skip' | 'zero' | 'null';

export interface RelativeOHLCOptions {
  handle_zero_division?: HandleZeroDivision;
}

export interface OHLCVResampleRequest {
  stock_code: string;
  source?: 'market' | 'dataset';
  timeframe?: Timeframe;
  start_date?: string;
  end_date?: string;
  benchmark_code?: string;
  relative_options?: RelativeOHLCOptions;
}

export interface OHLCVRecord {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface OHLCVResampleResponse {
  stock_code: string;
  timeframe: string;
  benchmark_code?: string;
  meta: {
    source_bars: number;
    resampled_bars: number;
  };
  data: OHLCVRecord[];
}
