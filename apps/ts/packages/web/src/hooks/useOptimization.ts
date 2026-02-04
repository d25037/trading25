/**
 * TanStack Query hooks for Optimization API
 */
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { apiDelete, apiGet, apiPost, apiPut } from '@/lib/api-client';
import type {
  HtmlFileDeleteResponse,
  HtmlFileRenameRequest,
  HtmlFileRenameResponse,
  OptimizationGridConfig,
  OptimizationGridListResponse,
  OptimizationGridSaveRequest,
  OptimizationGridSaveResponse,
  OptimizationHtmlFileContentResponse,
  OptimizationHtmlFileListResponse,
  OptimizationJobResponse,
  OptimizationRequest,
} from '@/types/backtest';
import { logger } from '@/utils/logger';

// Query Keys
export const optimizationKeys = {
  all: ['optimization'] as const,
  job: (jobId: string) => [...optimizationKeys.all, 'job', jobId] as const,
  gridConfigs: () => [...optimizationKeys.all, 'grid-configs'] as const,
  gridConfig: (strategy: string) => [...optimizationKeys.all, 'grid-config', strategy] as const,
  htmlFilesPrefix: () => [...optimizationKeys.all, 'html-files'] as const,
  htmlFiles: (strategy?: string) => [...optimizationKeys.all, 'html-files', strategy] as const,
  htmlFileContent: (strategy: string, filename: string) =>
    [...optimizationKeys.all, 'html-file-content', strategy, filename] as const,
};

// Fetch functions
function runOptimization(request: OptimizationRequest): Promise<OptimizationJobResponse> {
  return apiPost<OptimizationJobResponse>('/bt/api/optimize/run', request);
}

function fetchOptimizationJobStatus(jobId: string): Promise<OptimizationJobResponse> {
  return apiGet<OptimizationJobResponse>(`/bt/api/optimize/jobs/${encodeURIComponent(jobId)}`);
}

function fetchGridConfigs(): Promise<OptimizationGridListResponse> {
  return apiGet<OptimizationGridListResponse>('/bt/api/optimize/grid-configs');
}

function fetchGridConfig(strategy: string): Promise<OptimizationGridConfig> {
  return apiGet<OptimizationGridConfig>(`/bt/api/optimize/grid-configs/${encodeURIComponent(strategy)}`);
}

function saveGridConfig(strategy: string, request: OptimizationGridSaveRequest): Promise<OptimizationGridSaveResponse> {
  return apiPut<OptimizationGridSaveResponse>(`/bt/api/optimize/grid-configs/${encodeURIComponent(strategy)}`, request);
}

function deleteGridConfig(strategy: string): Promise<{ success: boolean; strategy_name: string }> {
  return apiDelete<{ success: boolean; strategy_name: string }>(
    `/bt/api/optimize/grid-configs/${encodeURIComponent(strategy)}`
  );
}

function fetchOptimizationHtmlFiles(strategy?: string, limit = 100): Promise<OptimizationHtmlFileListResponse> {
  const params = new URLSearchParams();
  if (strategy) params.append('strategy', strategy);
  params.append('limit', limit.toString());
  return apiGet<OptimizationHtmlFileListResponse>(`/bt/api/optimize/html-files?${params.toString()}`);
}

function fetchOptimizationHtmlFileContent(
  strategy: string,
  filename: string
): Promise<OptimizationHtmlFileContentResponse> {
  return apiGet<OptimizationHtmlFileContentResponse>(
    `/bt/api/optimize/html-files/${encodeURIComponent(strategy)}/${encodeURIComponent(filename)}`
  );
}

function renameOptimizationHtmlFile(
  strategy: string,
  filename: string,
  request: HtmlFileRenameRequest
): Promise<HtmlFileRenameResponse> {
  return apiPost<HtmlFileRenameResponse>(
    `/bt/api/optimize/html-files/${encodeURIComponent(strategy)}/${encodeURIComponent(filename)}/rename`,
    request
  );
}

function deleteOptimizationHtmlFile(strategy: string, filename: string): Promise<HtmlFileDeleteResponse> {
  return apiDelete<HtmlFileDeleteResponse>(
    `/bt/api/optimize/html-files/${encodeURIComponent(strategy)}/${encodeURIComponent(filename)}`
  );
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
 * Get all grid configs
 */
export function useOptimizationGridConfigs() {
  return useQuery({
    queryKey: optimizationKeys.gridConfigs(),
    queryFn: () => {
      logger.debug('Fetching grid configs');
      return fetchGridConfigs();
    },
    staleTime: 60 * 1000, // 1 minute
  });
}

/**
 * Get grid config for a specific strategy
 */
export function useOptimizationGridConfig(strategy: string | null) {
  return useQuery({
    queryKey: optimizationKeys.gridConfig(strategy ?? ''),
    queryFn: () => {
      if (!strategy) throw new Error('Strategy name required');
      logger.debug('Fetching grid config', { strategy });
      return fetchGridConfig(strategy);
    },
    enabled: !!strategy,
    staleTime: 60 * 1000, // 1 minute
    retry: false, // 404 is expected for strategies without grid config
  });
}

/**
 * Save grid config mutation
 */
export function useSaveOptimizationGrid() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ strategy, request }: { strategy: string; request: OptimizationGridSaveRequest }) =>
      saveGridConfig(strategy, request),
    onSuccess: (_data, { strategy }) => {
      logger.debug('Grid config saved', { strategy });
      queryClient.invalidateQueries({ queryKey: optimizationKeys.gridConfigs() });
      queryClient.invalidateQueries({ queryKey: optimizationKeys.gridConfig(strategy) });
    },
    onError: (error) => {
      logger.error('Failed to save grid config', { error: error.message });
    },
  });
}

/**
 * Delete grid config mutation
 */
export function useDeleteOptimizationGrid() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (strategy: string) => deleteGridConfig(strategy),
    onSuccess: (_data, strategy) => {
      logger.debug('Grid config deleted', { strategy });
      queryClient.invalidateQueries({ queryKey: optimizationKeys.gridConfigs() });
      queryClient.removeQueries({ queryKey: optimizationKeys.gridConfig(strategy) });
    },
    onError: (error) => {
      logger.error('Failed to delete grid config', { error: error.message });
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
