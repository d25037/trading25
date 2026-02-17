/**
 * TanStack Query hooks for Backtest API
 */
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { ApiError, apiDelete, apiGet, apiPost, apiPut } from '@/lib/api-client';
import type {
  AttributionArtifactContentResponse,
  AttributionArtifactListResponse,
  BacktestJobResponse,
  BacktestRequest,
  BacktestResultResponse,
  DefaultConfigResponse,
  DefaultConfigUpdateRequest,
  DefaultConfigUpdateResponse,
  HealthResponse,
  HtmlFileContentResponse,
  HtmlFileDeleteResponse,
  HtmlFileListResponse,
  HtmlFileRenameRequest,
  HtmlFileRenameResponse,
  SignalAttributionJobResponse,
  SignalAttributionRequest,
  SignalAttributionResultResponse,
  SignalReferenceResponse,
  StrategyDeleteResponse,
  StrategyDetailResponse,
  StrategyDuplicateRequest,
  StrategyDuplicateResponse,
  StrategyListResponse,
  StrategyMoveRequest,
  StrategyMoveResponse,
  StrategyRenameRequest,
  StrategyRenameResponse,
  StrategyUpdateRequest,
  StrategyUpdateResponse,
  StrategyValidationRequest,
  StrategyValidationResponse,
} from '@/types/backtest';
import { logger } from '@/utils/logger';

// Query Keys
export const backtestKeys = {
  all: ['backtest'] as const,
  health: () => [...backtestKeys.all, 'health'] as const,
  strategies: () => [...backtestKeys.all, 'strategies'] as const,
  strategy: (name: string) => [...backtestKeys.all, 'strategies', name] as const,
  jobs: (limit?: number) => [...backtestKeys.all, 'jobs', limit] as const,
  job: (jobId: string) => [...backtestKeys.all, 'job', jobId] as const,
  result: (jobId: string, includeHtml?: boolean) => [...backtestKeys.all, 'result', jobId, includeHtml] as const,
  attributionJob: (jobId: string) => [...backtestKeys.all, 'attribution-job', jobId] as const,
  attributionResult: (jobId: string) => [...backtestKeys.all, 'attribution-result', jobId] as const,
  attributionArtifactFiles: (strategy?: string, limit?: number) =>
    [...backtestKeys.all, 'attribution-artifact-files', strategy, limit] as const,
  attributionArtifactContent: (strategy: string, filename: string) =>
    [...backtestKeys.all, 'attribution-artifact-content', strategy, filename] as const,
  htmlFiles: (strategy?: string) => [...backtestKeys.all, 'html-files', strategy] as const,
  htmlFileContent: (strategy: string, filename: string) =>
    [...backtestKeys.all, 'html-file-content', strategy, filename] as const,
  defaultConfig: () => [...backtestKeys.all, 'default-config'] as const,
  signalReference: () => [...backtestKeys.all, 'signal-reference'] as const,
};

// Fetch functions
function fetchHealth(): Promise<HealthResponse> {
  return apiGet<HealthResponse>('/api/health');
}

function fetchStrategies(): Promise<StrategyListResponse> {
  return apiGet<StrategyListResponse>('/api/strategies');
}

function fetchStrategy(name: string): Promise<StrategyDetailResponse> {
  return apiGet<StrategyDetailResponse>(`/api/strategies/${encodeURIComponent(name)}`);
}

function fetchJobs(limit = 50): Promise<BacktestJobResponse[]> {
  return apiGet<BacktestJobResponse[]>(`/api/backtest/jobs?limit=${limit}`);
}

function fetchJobStatus(jobId: string): Promise<BacktestJobResponse> {
  return apiGet<BacktestJobResponse>(`/api/backtest/jobs/${encodeURIComponent(jobId)}`);
}

function fetchResult(jobId: string, includeHtml = false): Promise<BacktestResultResponse> {
  const params = includeHtml ? '?include_html=true' : '';
  return apiGet<BacktestResultResponse>(`/api/backtest/result/${encodeURIComponent(jobId)}${params}`);
}

function runBacktest(request: BacktestRequest): Promise<BacktestJobResponse> {
  return apiPost<BacktestJobResponse>('/api/backtest/run', request);
}

function runSignalAttribution(request: SignalAttributionRequest): Promise<SignalAttributionJobResponse> {
  return apiPost<SignalAttributionJobResponse>('/api/backtest/attribution/run', request);
}

function fetchSignalAttributionJobStatus(jobId: string): Promise<SignalAttributionJobResponse> {
  return apiGet<SignalAttributionJobResponse>(`/api/backtest/attribution/jobs/${encodeURIComponent(jobId)}`);
}

function fetchSignalAttributionResult(jobId: string): Promise<SignalAttributionResultResponse> {
  return apiGet<SignalAttributionResultResponse>(`/api/backtest/attribution/result/${encodeURIComponent(jobId)}`);
}

function fetchAttributionArtifactFiles(strategy?: string, limit = 100): Promise<AttributionArtifactListResponse> {
  const params = new URLSearchParams();
  if (strategy) params.append('strategy', strategy);
  params.append('limit', limit.toString());
  return apiGet<AttributionArtifactListResponse>(`/api/backtest/attribution-files?${params.toString()}`);
}

function fetchAttributionArtifactContent(
  strategy: string,
  filename: string
): Promise<AttributionArtifactContentResponse> {
  const params = new URLSearchParams({
    strategy,
    filename,
  });
  return apiGet<AttributionArtifactContentResponse>(`/api/backtest/attribution-files/content?${params.toString()}`);
}

function fetchHtmlFiles(strategy?: string, limit = 100): Promise<HtmlFileListResponse> {
  const params = new URLSearchParams();
  if (strategy) params.append('strategy', strategy);
  params.append('limit', limit.toString());
  return apiGet<HtmlFileListResponse>(`/api/backtest/html-files?${params.toString()}`);
}

function fetchHtmlFileContent(strategy: string, filename: string): Promise<HtmlFileContentResponse> {
  return apiGet<HtmlFileContentResponse>(
    `/api/backtest/html-files/${encodeURIComponent(strategy)}/${encodeURIComponent(filename)}`
  );
}

function renameHtmlFile(
  strategy: string,
  filename: string,
  request: HtmlFileRenameRequest
): Promise<HtmlFileRenameResponse> {
  return apiPost<HtmlFileRenameResponse>(
    `/api/backtest/html-files/${encodeURIComponent(strategy)}/${encodeURIComponent(filename)}/rename`,
    request
  );
}

function deleteHtmlFile(strategy: string, filename: string): Promise<HtmlFileDeleteResponse> {
  return apiDelete<HtmlFileDeleteResponse>(
    `/api/backtest/html-files/${encodeURIComponent(strategy)}/${encodeURIComponent(filename)}`
  );
}

// Hooks

/**
 * Check backtest server health
 */
export function useBacktestHealth() {
  return useQuery({
    queryKey: backtestKeys.health(),
    queryFn: fetchHealth,
    staleTime: 30 * 1000, // 30 seconds
    retry: 1,
  });
}

/**
 * Get all strategies
 */
export function useStrategies() {
  return useQuery({
    queryKey: backtestKeys.strategies(),
    queryFn: () => {
      logger.debug('Fetching strategies');
      return fetchStrategies();
    },
    staleTime: 5 * 60 * 1000, // 5 minutes
  });
}

/**
 * Get strategy details by name
 */
export function useStrategy(name: string | null) {
  return useQuery({
    queryKey: backtestKeys.strategy(name ?? ''),
    queryFn: () => {
      if (!name) throw new Error('Strategy name required');
      logger.debug('Fetching strategy', { name });
      return fetchStrategy(name);
    },
    enabled: !!name,
    staleTime: 5 * 60 * 1000, // 5 minutes
  });
}

/**
 * Get list of backtest jobs
 */
export function useJobs(limit?: number) {
  return useQuery({
    queryKey: backtestKeys.jobs(limit),
    queryFn: () => {
      logger.debug('Fetching jobs', { limit });
      return fetchJobs(limit);
    },
    staleTime: 10 * 1000, // 10 seconds
  });
}

/**
 * Get job status with polling
 */
export function useJobStatus(jobId: string | null) {
  return useQuery({
    queryKey: backtestKeys.job(jobId ?? ''),
    queryFn: () => {
      if (!jobId) throw new Error('Job ID required');
      logger.debug('Polling job status', { jobId });
      return fetchJobStatus(jobId);
    },
    enabled: !!jobId,
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      if (status === 'running' || status === 'pending') return 2000;
      return false;
    },
    staleTime: 0,
  });
}

/**
 * Get backtest result
 */
export function useBacktestResult(jobId: string | null, includeHtml = false) {
  return useQuery({
    queryKey: backtestKeys.result(jobId ?? '', includeHtml),
    queryFn: () => {
      if (!jobId) throw new Error('Job ID required');
      logger.debug('Fetching result', { jobId, includeHtml });
      return fetchResult(jobId, includeHtml);
    },
    enabled: !!jobId,
    staleTime: Infinity, // Results don't change
  });
}

/**
 * Run backtest mutation
 */
export function useRunBacktest() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: runBacktest,
    onSuccess: (data) => {
      logger.debug('Backtest started', { jobId: data.job_id, status: data.status });
      queryClient.invalidateQueries({ queryKey: backtestKeys.jobs() });
    },
    onError: (error) => {
      logger.error('Failed to start backtest', { error: error.message });
    },
  });
}

export function useRunSignalAttribution() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: runSignalAttribution,
    onSuccess: (data) => {
      logger.debug('Signal attribution started', { jobId: data.job_id, status: data.status });
      queryClient.invalidateQueries({ queryKey: backtestKeys.attributionJob(data.job_id) });
    },
    onError: (error) => {
      logger.error('Failed to start signal attribution', { error: error.message });
    },
  });
}

export function useSignalAttributionJobStatus(jobId: string | null) {
  return useQuery({
    queryKey: backtestKeys.attributionJob(jobId ?? ''),
    queryFn: () => {
      if (!jobId) throw new Error('Job ID required');
      logger.debug('Polling attribution job status', { jobId });
      return fetchSignalAttributionJobStatus(jobId);
    },
    enabled: !!jobId,
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      if (status === 'running' || status === 'pending') return 2000;
      return false;
    },
    staleTime: 0,
  });
}

export function useSignalAttributionResult(jobId: string | null) {
  return useQuery({
    queryKey: backtestKeys.attributionResult(jobId ?? ''),
    queryFn: () => {
      if (!jobId) throw new Error('Job ID required');
      logger.debug('Fetching attribution result', { jobId });
      return fetchSignalAttributionResult(jobId);
    },
    enabled: !!jobId,
    staleTime: Infinity,
  });
}

export function useAttributionArtifactFiles(strategy?: string, limit = 100) {
  return useQuery({
    queryKey: backtestKeys.attributionArtifactFiles(strategy, limit),
    queryFn: () => {
      logger.debug('Fetching attribution artifact files', { strategy, limit });
      return fetchAttributionArtifactFiles(strategy, limit);
    },
    staleTime: 60 * 1000, // 1 minute
  });
}

export function useAttributionArtifactContent(strategy: string | null, filename: string | null) {
  return useQuery({
    queryKey: backtestKeys.attributionArtifactContent(strategy ?? '', filename ?? ''),
    queryFn: () => {
      if (!strategy || !filename) throw new Error('Strategy and filename required');
      logger.debug('Fetching attribution artifact content', { strategy, filename });
      return fetchAttributionArtifactContent(strategy, filename);
    },
    enabled: !!strategy && !!filename,
    staleTime: Infinity,
  });
}

// ============================================
// Cancel Backtest
// ============================================

function cancelJob(jobId: string): Promise<BacktestJobResponse> {
  return apiPost<BacktestJobResponse>(`/api/backtest/jobs/${encodeURIComponent(jobId)}/cancel`);
}

/**
 * Cancel a running backtest job
 */
export function useCancelBacktest() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: cancelJob,
    onMutate: async (jobId) => {
      // Cancel in-flight polling queries to avoid race condition
      await queryClient.cancelQueries({ queryKey: backtestKeys.job(jobId) });
    },
    onSuccess: (data) => {
      logger.debug('Backtest cancelled', { jobId: data.job_id });
      queryClient.invalidateQueries({ queryKey: backtestKeys.job(data.job_id) });
      queryClient.invalidateQueries({ queryKey: backtestKeys.jobs() });
    },
    onError: (error) => {
      // 409 Conflict: job already completed/failed — refetch to get latest state
      if (error instanceof ApiError && error.status === 409) {
        logger.debug('Cancel rejected (job already terminal), refreshing state');
        queryClient.invalidateQueries({ queryKey: backtestKeys.jobs() });
        return;
      }
      logger.error('Failed to cancel backtest', { error: error.message });
    },
  });
}

function cancelSignalAttribution(jobId: string): Promise<SignalAttributionJobResponse> {
  return apiPost<SignalAttributionJobResponse>(`/api/backtest/attribution/jobs/${encodeURIComponent(jobId)}/cancel`);
}

export function useCancelSignalAttribution() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: cancelSignalAttribution,
    onMutate: async (jobId) => {
      await queryClient.cancelQueries({ queryKey: backtestKeys.attributionJob(jobId) });
    },
    onSuccess: (data) => {
      logger.debug('Signal attribution cancelled', { jobId: data.job_id });
      queryClient.invalidateQueries({ queryKey: backtestKeys.attributionJob(data.job_id) });
    },
    onError: (error, jobId) => {
      if (error instanceof ApiError && error.status === 409) {
        logger.debug('Cancel rejected (attribution job already terminal), refreshing state');
        queryClient.invalidateQueries({ queryKey: backtestKeys.attributionJob(jobId) });
        return;
      }
      logger.error('Failed to cancel signal attribution', { error: error.message });
    },
  });
}

// ============================================
// Strategy CRUD Mutations
// ============================================

function updateStrategy(name: string, request: StrategyUpdateRequest): Promise<StrategyUpdateResponse> {
  return apiPut<StrategyUpdateResponse>(`/api/strategies/${encodeURIComponent(name)}`, request);
}

function deleteStrategy(name: string): Promise<StrategyDeleteResponse> {
  return apiDelete<StrategyDeleteResponse>(`/api/strategies/${encodeURIComponent(name)}`);
}

function duplicateStrategy(name: string, request: StrategyDuplicateRequest): Promise<StrategyDuplicateResponse> {
  return apiPost<StrategyDuplicateResponse>(`/api/strategies/${encodeURIComponent(name)}/duplicate`, request);
}

function renameStrategy(name: string, request: StrategyRenameRequest): Promise<StrategyRenameResponse> {
  return apiPost<StrategyRenameResponse>(`/api/strategies/${encodeURIComponent(name)}/rename`, request);
}

function moveStrategy(name: string, request: StrategyMoveRequest): Promise<StrategyMoveResponse> {
  return apiPost<StrategyMoveResponse>(`/api/strategies/${encodeURIComponent(name)}/move`, request);
}

function validateStrategy(name: string, request: StrategyValidationRequest): Promise<StrategyValidationResponse> {
  return apiPost<StrategyValidationResponse>(`/api/strategies/${encodeURIComponent(name)}/validate`, request);
}

/**
 * Update strategy config mutation
 */
export function useUpdateStrategy() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ name, request }: { name: string; request: StrategyUpdateRequest }) => updateStrategy(name, request),
    onSuccess: (_data, { name }) => {
      logger.debug('Strategy updated', { name });
      queryClient.invalidateQueries({ queryKey: backtestKeys.strategies() });
      queryClient.invalidateQueries({ queryKey: backtestKeys.strategy(name) });
    },
    onError: (error) => {
      logger.error('Failed to update strategy', { error: error.message });
    },
  });
}

/**
 * Delete strategy mutation
 */
export function useDeleteStrategy() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: deleteStrategy,
    onSuccess: (_data, name) => {
      logger.debug('Strategy deleted', { name });
      queryClient.invalidateQueries({ queryKey: backtestKeys.strategies() });
      queryClient.removeQueries({ queryKey: backtestKeys.strategy(name) });
    },
    onError: (error) => {
      logger.error('Failed to delete strategy', { error: error.message });
    },
  });
}

/**
 * Duplicate strategy mutation
 */
export function useDuplicateStrategy() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ name, request }: { name: string; request: StrategyDuplicateRequest }) =>
      duplicateStrategy(name, request),
    onSuccess: (data) => {
      logger.debug('Strategy duplicated', { newName: data.new_strategy_name });
      queryClient.invalidateQueries({ queryKey: backtestKeys.strategies() });
    },
    onError: (error) => {
      logger.error('Failed to duplicate strategy', { error: error.message });
    },
  });
}

/**
 * Rename strategy mutation
 */
export function useRenameStrategy() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ name, request }: { name: string; request: StrategyRenameRequest }) => renameStrategy(name, request),
    onSuccess: (data, { name }) => {
      logger.debug('Strategy renamed', { oldName: name, newName: data.new_name });
      queryClient.invalidateQueries({ queryKey: backtestKeys.strategies() });
      queryClient.removeQueries({ queryKey: backtestKeys.strategy(name) });
    },
    onError: (error) => {
      logger.error('Failed to rename strategy', { error: error.message });
    },
  });
}

/**
 * Move strategy across managed categories
 */
export function useMoveStrategy() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ name, request }: { name: string; request: StrategyMoveRequest }) => moveStrategy(name, request),
    onSuccess: (data, { name }) => {
      logger.debug('Strategy moved', { oldName: name, newName: data.new_strategy_name, to: data.target_category });
      queryClient.invalidateQueries({ queryKey: backtestKeys.strategies() });
      queryClient.removeQueries({ queryKey: backtestKeys.strategy(name) });
      queryClient.invalidateQueries({ queryKey: backtestKeys.strategy(data.new_strategy_name) });
    },
    onError: (error) => {
      logger.error('Failed to move strategy', { error: error.message });
    },
  });
}

/**
 * Validate strategy config mutation
 */
export function useValidateStrategy() {
  return useMutation({
    mutationFn: ({ name, request }: { name: string; request: StrategyValidationRequest }) =>
      validateStrategy(name, request),
    onError: (error) => {
      logger.error('Failed to validate strategy', { error: error.message });
    },
  });
}

// ============================================
// HTML File Browser Hooks
// ============================================

/**
 * Get list of HTML result files
 */
export function useHtmlFiles(strategy?: string, limit = 100) {
  return useQuery({
    queryKey: backtestKeys.htmlFiles(strategy),
    queryFn: () => {
      logger.debug('Fetching HTML files', { strategy, limit });
      return fetchHtmlFiles(strategy, limit);
    },
    staleTime: 60 * 1000, // 1 minute
  });
}

/**
 * Get HTML file content
 */
export function useHtmlFileContent(strategy: string | null, filename: string | null) {
  return useQuery({
    queryKey: backtestKeys.htmlFileContent(strategy ?? '', filename ?? ''),
    queryFn: () => {
      if (!strategy || !filename) throw new Error('Strategy and filename required');
      logger.debug('Fetching HTML file content', { strategy, filename });
      return fetchHtmlFileContent(strategy, filename);
    },
    enabled: !!strategy && !!filename,
    staleTime: Infinity, // Files don't change
  });
}

/**
 * Rename HTML file mutation
 */
export function useRenameHtmlFile() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({
      strategy,
      filename,
      request,
    }: {
      strategy: string;
      filename: string;
      request: HtmlFileRenameRequest;
    }) => renameHtmlFile(strategy, filename, request),
    onSuccess: (data) => {
      logger.debug('HTML file renamed', {
        old: data.old_filename,
        new: data.new_filename,
      });
      queryClient.invalidateQueries({ queryKey: backtestKeys.htmlFiles() });
      queryClient.removeQueries({
        queryKey: backtestKeys.htmlFileContent(data.strategy_name, data.old_filename),
      });
    },
    onError: (error) => {
      logger.error('Failed to rename HTML file', { error: error.message });
    },
  });
}

/**
 * Delete HTML file mutation
 */
export function useDeleteHtmlFile() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ strategy, filename }: { strategy: string; filename: string }) => deleteHtmlFile(strategy, filename),
    onSuccess: (data) => {
      logger.debug('HTML file deleted', {
        strategy: data.strategy_name,
        filename: data.filename,
      });
      queryClient.invalidateQueries({ queryKey: backtestKeys.htmlFiles() });
      queryClient.removeQueries({
        queryKey: backtestKeys.htmlFileContent(data.strategy_name, data.filename),
      });
    },
    onError: (error) => {
      logger.error('Failed to delete HTML file', { error: error.message });
    },
  });
}

// ============================================
// Default Config Hooks
// ============================================

function fetchDefaultConfig(): Promise<DefaultConfigResponse> {
  return apiGet<DefaultConfigResponse>('/api/config/default');
}

function updateDefaultConfig(request: DefaultConfigUpdateRequest): Promise<DefaultConfigUpdateResponse> {
  return apiPut<DefaultConfigUpdateResponse>('/api/config/default', request);
}

/**
 * Get default config (raw YAML string)
 */
export function useDefaultConfig(enabled = true) {
  return useQuery({
    queryKey: backtestKeys.defaultConfig(),
    queryFn: () => {
      logger.debug('Fetching default config');
      return fetchDefaultConfig();
    },
    enabled,
    staleTime: 5 * 60 * 1000, // 5 minutes
  });
}

/**
 * Update default config mutation
 */
export function useUpdateDefaultConfig() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: updateDefaultConfig,
    onSuccess: () => {
      logger.debug('Default config updated');
      queryClient.invalidateQueries({ queryKey: backtestKeys.defaultConfig() });
    },
    onError: (error) => {
      logger.error('Failed to update default config', { error: error.message });
    },
  });
}

// ============================================
// Signal Reference Hooks
// ============================================

function fetchSignalReference(): Promise<SignalReferenceResponse> {
  return apiGet<SignalReferenceResponse>('/api/signals/reference');
}

/**
 * Get signal reference data (static data, long stale time)
 */
export function useSignalReference() {
  return useQuery({
    queryKey: backtestKeys.signalReference(),
    queryFn: () => {
      logger.debug('Fetching signal reference');
      return fetchSignalReference();
    },
    staleTime: 30 * 60 * 1000, // 30 minutes
  });
}
