/**
 * TanStack Query hooks for Optimization API
 */
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { backtestClient } from '@/lib/backtest-client';
import type {
  HtmlFileDeleteResponse,
  HtmlFileRenameRequest,
  HtmlFileRenameResponse,
  OptimizationHtmlFileContentResponse,
  OptimizationHtmlFileListResponse,
  OptimizationJobResponse,
  OptimizationRequest,
  StrategyOptimizationSaveRequest,
  StrategyOptimizationSaveResponse,
  StrategyOptimizationStateResponse,
} from '@/types/backtest';
import { logger } from '@/utils/logger';

// Query Keys
export const optimizationKeys = {
  all: ['optimization'] as const,
  job: (jobId: string) => [...optimizationKeys.all, 'job', jobId] as const,
  strategySpec: (strategy: string) => [...optimizationKeys.all, 'strategy-spec', strategy] as const,
  htmlFilesPrefix: () => [...optimizationKeys.all, 'html-files'] as const,
  htmlFiles: (strategy?: string) => [...optimizationKeys.all, 'html-files', strategy] as const,
  htmlFileContent: (strategy: string, filename: string) =>
    [...optimizationKeys.all, 'html-file-content', strategy, filename] as const,
};

// Fetch functions
function runOptimization(request: OptimizationRequest): Promise<OptimizationJobResponse> {
  return backtestClient.runOptimization(request);
}

function fetchOptimizationJobStatus(jobId: string): Promise<OptimizationJobResponse> {
  return backtestClient.getOptimizationJobStatus(jobId);
}

function cancelOptimizationJob(jobId: string): Promise<OptimizationJobResponse> {
  return backtestClient.cancelOptimizationJob(jobId);
}

function fetchStrategyOptimization(strategy: string): Promise<StrategyOptimizationStateResponse> {
  return backtestClient.getStrategyOptimization(strategy);
}

function generateStrategyOptimizationDraft(strategy: string): Promise<StrategyOptimizationStateResponse> {
  return backtestClient.generateStrategyOptimizationDraft(strategy);
}

function saveStrategyOptimization(
  strategy: string,
  request: StrategyOptimizationSaveRequest
): Promise<StrategyOptimizationSaveResponse> {
  return backtestClient.saveStrategyOptimization(strategy, request);
}

function deleteStrategyOptimization(strategy: string) {
  return backtestClient.deleteStrategyOptimization(strategy);
}

function fetchOptimizationHtmlFiles(strategy?: string, limit = 100): Promise<OptimizationHtmlFileListResponse> {
  return backtestClient.listOptimizationHtmlFiles({ strategy, limit });
}

function fetchOptimizationHtmlFileContent(
  strategy: string,
  filename: string
): Promise<OptimizationHtmlFileContentResponse> {
  return backtestClient.getOptimizationHtmlFileContent(strategy, filename);
}

function renameOptimizationHtmlFile(
  strategy: string,
  filename: string,
  request: HtmlFileRenameRequest
): Promise<HtmlFileRenameResponse> {
  return backtestClient.renameOptimizationHtmlFile(strategy, filename, request);
}

function deleteOptimizationHtmlFile(strategy: string, filename: string): Promise<HtmlFileDeleteResponse> {
  return backtestClient.deleteOptimizationHtmlFile(strategy, filename);
}

// Hooks

/**
 * Run optimization mutation
 */
export function useRunOptimization() {
  return useMutation({
    mutationFn: runOptimization,
    onSuccess: (data) => {
      logger.debug('Optimization started', { jobId: data.job_id, status: data.status });
    },
    onError: (error) => {
      logger.error('Failed to start optimization', { error: error.message });
    },
  });
}

/**
 * Get optimization job status with polling
 */
export function useOptimizationJobStatus(jobId: string | null) {
  return useQuery({
    queryKey: optimizationKeys.job(jobId ?? ''),
    queryFn: () => {
      if (!jobId) throw new Error('Job ID required');
      logger.debug('Polling optimization job status', { jobId });
      return fetchOptimizationJobStatus(jobId);
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
 * Cancel optimization mutation
 */
export function useCancelOptimization() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (jobId: string) => cancelOptimizationJob(jobId),
    onSuccess: (data, jobId) => {
      logger.debug('Optimization cancelled', { jobId: data.job_id, status: data.status });
      queryClient.invalidateQueries({ queryKey: optimizationKeys.job(jobId) });
    },
    onError: (error) => {
      logger.error('Failed to cancel optimization', { error: error.message });
    },
  });
}

/**
 * Get strategy-linked optimization state
 */
export function useStrategyOptimization(strategy: string | null) {
  return useQuery({
    queryKey: optimizationKeys.strategySpec(strategy ?? ''),
    queryFn: () => {
      if (!strategy) throw new Error('Strategy name required');
      logger.debug('Fetching strategy optimization state', { strategy });
      return fetchStrategyOptimization(strategy);
    },
    enabled: !!strategy,
    staleTime: 60 * 1000, // 1 minute
    retry: false,
  });
}

/**
 * Generate strategy-linked optimization draft
 */
export function useGenerateStrategyOptimizationDraft() {
  return useMutation({
    mutationFn: (strategy: string) => generateStrategyOptimizationDraft(strategy),
    onSuccess: (data, strategy) => {
      logger.debug('Generated strategy optimization draft', {
        strategy,
        persisted: data.persisted,
        readyToRun: data.ready_to_run,
      });
    },
    onError: (error) => {
      logger.error('Failed to generate strategy optimization draft', { error: error.message });
    },
  });
}

/**
 * Save strategy optimization mutation
 */
export function useSaveStrategyOptimization() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ strategy, request }: { strategy: string; request: StrategyOptimizationSaveRequest }) =>
      saveStrategyOptimization(strategy, request),
    onSuccess: (_data, { strategy }) => {
      logger.debug('Strategy optimization saved', { strategy });
      queryClient.invalidateQueries({ queryKey: optimizationKeys.strategySpec(strategy) });
    },
    onError: (error) => {
      logger.error('Failed to save strategy optimization', { error: error.message });
    },
  });
}

/**
 * Delete strategy optimization mutation
 */
export function useDeleteStrategyOptimization() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (strategy: string) => deleteStrategyOptimization(strategy),
    onSuccess: (_data, strategy) => {
      logger.debug('Strategy optimization deleted', { strategy });
      queryClient.invalidateQueries({ queryKey: optimizationKeys.strategySpec(strategy) });
    },
    onError: (error) => {
      logger.error('Failed to delete strategy optimization', { error: error.message });
    },
  });
}

/**
 * Get optimization HTML result files
 */
export function useOptimizationHtmlFiles(strategy?: string, limit = 100) {
  return useQuery({
    queryKey: optimizationKeys.htmlFiles(strategy),
    queryFn: () => {
      logger.debug('Fetching optimization HTML files', { strategy, limit });
      return fetchOptimizationHtmlFiles(strategy, limit);
    },
    staleTime: 60 * 1000, // 1 minute
  });
}

/**
 * Get optimization HTML file content
 */
export function useOptimizationHtmlFileContent(strategy: string | null, filename: string | null) {
  return useQuery({
    queryKey: optimizationKeys.htmlFileContent(strategy ?? '', filename ?? ''),
    queryFn: () => {
      if (!strategy || !filename) throw new Error('Strategy and filename required');
      logger.debug('Fetching optimization HTML file content', { strategy, filename });
      return fetchOptimizationHtmlFileContent(strategy, filename);
    },
    enabled: !!strategy && !!filename,
    staleTime: Infinity, // Files don't change
  });
}

/**
 * Rename optimization HTML file mutation
 */
export function useRenameOptimizationHtmlFile() {
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
    }) => renameOptimizationHtmlFile(strategy, filename, request),
    onSuccess: (data) => {
      logger.debug('Optimization HTML file renamed', {
        old: data.old_filename,
        new: data.new_filename,
      });
      queryClient.invalidateQueries({ queryKey: optimizationKeys.htmlFilesPrefix() });
      queryClient.removeQueries({
        queryKey: optimizationKeys.htmlFileContent(data.strategy_name, data.old_filename),
      });
    },
    onError: (error) => {
      logger.error('Failed to rename optimization HTML file', { error: error.message });
    },
  });
}

/**
 * Delete optimization HTML file mutation
 */
export function useDeleteOptimizationHtmlFile() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ strategy, filename }: { strategy: string; filename: string }) =>
      deleteOptimizationHtmlFile(strategy, filename),
    onSuccess: (data) => {
      logger.debug('Optimization HTML file deleted', {
        strategy: data.strategy_name,
        filename: data.filename,
      });
      queryClient.invalidateQueries({ queryKey: optimizationKeys.htmlFilesPrefix() });
      queryClient.removeQueries({
        queryKey: optimizationKeys.htmlFileContent(data.strategy_name, data.filename),
      });
    },
    onError: (error) => {
      logger.error('Failed to delete optimization HTML file', { error: error.message });
    },
  });
}
