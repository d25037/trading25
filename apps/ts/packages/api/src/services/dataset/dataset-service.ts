import { Database as SQLiteDatabase } from 'bun:sqlite';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  createDebugConfig,
  DatasetBuilder,
  DatasetReader,
  getPresetConfig,
  getPresetEstimatedTime,
  getPresetStockRange,
  isValidPreset,
  type ProgressInfo,
} from '@trading25/shared/dataset';
import { getDatasetPath } from '@trading25/shared/utils/dataset-paths';
import { logger } from '@trading25/shared/utils/logger';
import type {
  CancelDatasetJobResponse,
  CreateDatasetJobResponse,
  DatasetDeleteResponse,
  DatasetInfoResponse,
  DatasetJobResponse,
  DatasetListResponse,
  DatasetPreset,
  DatasetSampleQuery,
  DatasetSampleResponse,
  DatasetSearchQuery,
  DatasetSearchResponse,
  DatasetValidationDetails,
} from '../../schemas/dataset';
import { createJQuantsClient } from '../../utils/jquants-client-factory';
import { type DatasetJob, datasetJobManager } from './dataset-job-manager';

/**
 * Job timeout configuration
 * Extended to 35 minutes to accommodate circuit breaker cooldowns (60s each)
 * With up to 3 cooldown periods, this allows for ~25 min processing + ~5 min cooldown buffer
 */
const JOB_TIMEOUT_MS = 35 * 60 * 1000; // 35 minutes
const STALL_CHECK_INTERVAL_MS = 60 * 1000; // Check every 1 minute
const STALL_THRESHOLD_MS = 3 * 60 * 1000; // 3 minutes without progress update

function jobToResponse(job: DatasetJob): DatasetJobResponse {
  return {
    jobId: job.jobId,
    status: job.status,
    name: job.name,
    preset: job.preset,
    progress: job.progress,
    result: job.result,
    startedAt: job.startedAt.toISOString(),
    completedAt: job.completedAt?.toISOString(),
    error: job.error,
  };
}

type SearchMatchType = 'code' | 'name' | 'english_name';

type StockInfo = {
  code: string;
  companyName: string;
  companyNameEnglish: string;
  marketName: string;
  sector33Name: string;
};

function resolveTimeoutMs(timeoutMinutes?: number): number {
  if (!timeoutMinutes) return JOB_TIMEOUT_MS;
  const clamped = Math.max(1, Math.min(timeoutMinutes, 120));
  return clamped * 60_000;
}

function matchStockExact(stock: StockInfo, term: string): SearchMatchType | null {
  if (stock.code === term) return 'code';
  if (stock.companyName === term) return 'name';
  if (stock.companyNameEnglish === term) return 'english_name';
  return null;
}

function matchStockPartial(stock: StockInfo, termLower: string): SearchMatchType | null {
  if (stock.code.toLowerCase().includes(termLower)) return 'code';
  if (stock.companyName.toLowerCase().includes(termLower)) return 'name';
  if (stock.companyNameEnglish.toLowerCase().includes(termLower)) return 'english_name';
  return null;
}

function transformToSearchResult(stock: StockInfo, matchType: SearchMatchType) {
  return {
    code: stock.code,
    companyName: stock.companyName,
    companyNameEnglish: stock.companyNameEnglish,
    marketName: stock.marketName,
    sectorName: stock.sector33Name,
    matchType,
  };
}

export class DatasetService {
  private getDatasetsDir(): string {
    const dataHome = process.env.XDG_DATA_HOME || path.join(os.homedir(), '.local', 'share');
    return path.join(dataHome, 'trading25', 'datasets');
  }

  listDatasets(): DatasetListResponse {
    const datasetsDir = this.getDatasetsDir();

    if (!fs.existsSync(datasetsDir)) {
      return { datasets: [], totalCount: 0 };
    }

    const entries = fs.readdirSync(datasetsDir);
    const dbFiles = entries.filter((f) => f.endsWith('.db'));

    const datasets = dbFiles.map((filename) => {
      const filePath = path.join(datasetsDir, filename);
      const stat = fs.statSync(filePath);

      let preset: string | null = null;
      let createdAt: string | null = null;
      try {
        // Use readonly connection to avoid WAL checkpoint updating mtime
        const sqlite = new SQLiteDatabase(filePath, { readonly: true });
        try {
          const row = sqlite
            .query<{ key: string; value: string }, []>(
              "SELECT key, value FROM dataset_info WHERE key IN ('preset', 'created_at')"
            )
            .all();
          for (const r of row) {
            if (r.key === 'preset') preset = r.value;
            if (r.key === 'created_at') createdAt = r.value;
          }
        } finally {
          sqlite.close();
        }
      } catch (error) {
        logger.warn('Failed to read dataset metadata', {
          filename,
          error: error instanceof Error ? error.message : 'Unknown error',
        });
      }

      return {
        name: filename,
        fileSize: stat.size,
        lastModified: stat.mtime.toISOString(),
        preset,
        createdAt,
      };
    });

    return { datasets, totalCount: datasets.length };
  }

  deleteDataset(name: string): DatasetDeleteResponse {
    const datasetPath = getDatasetPath(name);

    if (!fs.existsSync(datasetPath)) {
      return { success: false, name, message: `Dataset "${name}" not found` };
    }

    try {
      fs.unlinkSync(datasetPath);
      logger.info('Dataset deleted', { name, path: datasetPath });
      return { success: true, name, message: `Dataset "${name}" deleted successfully` };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logger.error('Failed to delete dataset', { name, error: errorMessage });
      return { success: false, name, message: `Failed to delete: ${errorMessage}` };
    }
  }

  private canStartJob(): boolean {
    const activeJob = datasetJobManager.getActiveJob();
    if (activeJob && (activeJob.status === 'pending' || activeJob.status === 'running')) {
      logger.warn('Dataset job already in progress', { activeJobId: activeJob.jobId });
      return false;
    }
    return true;
  }

  startCreateJob(
    name: string,
    preset: DatasetPreset,
    overwrite = false,
    timeoutMinutes?: number
  ): CreateDatasetJobResponse | null {
    if (!this.canStartJob()) return null;

    const outputPath = getDatasetPath(name);
    if (fs.existsSync(outputPath) && !overwrite) {
      logger.warn('Dataset already exists', { name, outputPath });
      return null;
    }

    const job = datasetJobManager.createJob(name, preset);
    if (!job) return null;

    this.executeCreateInBackground(job.jobId, name, preset, overwrite, timeoutMinutes);

    return {
      jobId: job.jobId,
      status: job.status,
      name: job.name,
      preset: job.preset,
      message: 'Dataset creation job started',
      estimatedTime: getPresetEstimatedTime(preset),
    };
  }

  startResumeJob(name: string, preset: DatasetPreset, timeoutMinutes?: number): CreateDatasetJobResponse | null {
    if (!this.canStartJob()) return null;

    const outputPath = getDatasetPath(name);
    if (!fs.existsSync(outputPath)) {
      logger.warn('Dataset does not exist for resume', { name, outputPath });
      return null;
    }

    const job = datasetJobManager.createJob(name, preset);
    if (!job) return null;

    this.executeResumeInBackground(job.jobId, name, preset, timeoutMinutes);

    return {
      jobId: job.jobId,
      status: job.status,
      name: job.name,
      preset: job.preset,
      message: 'Dataset resume job started',
      estimatedTime: 'Depends on missing data',
    };
  }

  getJobStatus(jobId: string): DatasetJobResponse | null {
    const job = datasetJobManager.getJob(jobId);
    if (!job) {
      return null;
    }
    return jobToResponse(job);
  }

  cancelJob(jobId: string): CancelDatasetJobResponse {
    const job = datasetJobManager.getJob(jobId);
    if (!job) {
      return { success: false, jobId, message: 'Job not found' };
    }

    if (job.status !== 'pending' && job.status !== 'running') {
      return { success: false, jobId, message: `Cannot cancel job in ${job.status} state` };
    }

    const cancelled = datasetJobManager.cancelJob(jobId);
    return {
      success: cancelled,
      jobId,
      message: cancelled ? 'Job cancelled successfully' : 'Failed to cancel job',
    };
  }

  private formatDateRange(dateRange: { from: Date; to: Date } | undefined): { from: string; to: string } {
    if (!dateRange) {
      return { from: '', to: '' };
    }
    const formatDate = (d: Date): string => d.toISOString().split('T')[0] || '';
    return {
      from: formatDate(dateRange.from),
      to: formatDate(dateRange.to),
    };
  }

  async getDatasetInfo(name: string): Promise<DatasetInfoResponse | null> {
    const datasetPath = getDatasetPath(name);

    if (!fs.existsSync(datasetPath)) {
      return null;
    }

    const reader = new DatasetReader(datasetPath);
    try {
      const stats = await reader.getDatasetStats();
      const fileStats = fs.statSync(datasetPath);
      const preset = await reader.getPreset();
      const createdAt = await reader.getCreatedAt();
      const validation = await this.performEnhancedValidation(reader, stats);
      const statementsFieldCoverage = stats.hasStatementsData ? await reader.getStatementsFieldCoverage() : null;

      if (statementsFieldCoverage && !statementsFieldCoverage.hasExtendedFields) {
        validation.errors.push(
          'Statements schema outdated: missing extended financial fields (bps, sales, etc.). Recreate dataset with --overwrite'
        );
        validation.isValid = false;
      }

      if (statementsFieldCoverage && !statementsFieldCoverage.hasCashFlowFields) {
        validation.warnings.push(
          'Cash flow fields not available (schema v1). Recreate with --overwrite to enable CF analysis'
        );
      }

      return {
        name,
        path: datasetPath,
        fileSize: fileStats.size,
        lastModified: fileStats.mtime.toISOString(),
        snapshot: {
          preset,
          createdAt,
        },
        stats: {
          totalStocks: stats.totalStocks,
          totalQuotes: stats.totalQuotes,
          dateRange: this.formatDateRange(stats.dateRange),
          hasMarginData: stats.hasMarginData ?? false,
          hasTOPIXData: stats.hasTOPIXData ?? false,
          hasSectorData: stats.hasSectorData ?? false,
          hasStatementsData: stats.hasStatementsData ?? false,
          statementsFieldCoverage,
        },
        validation,
      };
    } finally {
      await reader.close();
    }
  }

  private async validateStockCount(
    reader: InstanceType<typeof DatasetReader>,
    totalStocks: number,
    errors: string[],
    details: DatasetValidationDetails
  ): Promise<void> {
    const preset = await reader.getPreset();
    const expectedRange = preset && isValidPreset(preset) ? getPresetStockRange(preset) : null;
    const isWithinRange = expectedRange ? totalStocks >= expectedRange.min && totalStocks <= expectedRange.max : true;

    details.stockCountValidation = { preset, expected: expectedRange, actual: totalStocks, isWithinRange };
    if (expectedRange && !isWithinRange) {
      errors.push(
        `Stock count ${totalStocks} outside expected range (${expectedRange.min}-${expectedRange.max}) for preset '${preset}'`
      );
    }
  }

  private async checkDataCoverage(
    reader: InstanceType<typeof DatasetReader>,
    stats: { hasStatementsData: boolean; hasMarginData: boolean },
    warnings: string[],
    details: DatasetValidationDetails
  ): Promise<void> {
    const resumeStatus = await reader.getResumeStatus();
    details.dataCoverage = {
      totalStocks: resumeStatus.totalStocks,
      stocksWithQuotes: resumeStatus.totalStocks - resumeStatus.missingQuotes,
      stocksWithStatements: resumeStatus.totalStocks - resumeStatus.missingStatements,
      stocksWithMargin: resumeStatus.totalStocks - resumeStatus.missingMargin,
    };
    if (resumeStatus.missingStatements > 0 && stats.hasStatementsData) {
      warnings.push(`${resumeStatus.missingStatements} stocks missing statements data`);
    }
    if (resumeStatus.missingMargin > 0 && stats.hasMarginData) {
      warnings.push(`${resumeStatus.missingMargin} stocks missing margin data`);
    }
  }

  private async performEnhancedValidation(
    reader: InstanceType<typeof DatasetReader>,
    stats: Awaited<ReturnType<typeof reader.getDatasetStats>>
  ): Promise<{
    isValid: boolean;
    errors: string[];
    warnings: string[];
    details: DatasetValidationDetails;
  }> {
    const errors: string[] = [];
    const warnings: string[] = [];
    const details: DatasetValidationDetails = {};

    if (stats.totalStocks === 0) errors.push('Dataset contains no stocks');
    if (stats.totalQuotes === 0) errors.push('Dataset contains no quote records');
    if (!stats.dateRange) warnings.push('Date range information is incomplete');

    await this.validateStockCount(reader, stats.totalStocks, errors, details);

    const fkIssues = await reader.getFKIntegrityIssues();
    details.fkIntegrity = fkIssues;
    const totalOrphans = fkIssues.stockDataOrphans + fkIssues.marginDataOrphans + fkIssues.statementsOrphans;
    if (totalOrphans > 0) errors.push(`Referential integrity issues: ${totalOrphans} orphan records`);

    const orphanStocks = await reader.getOrphanStocksCount();
    details.orphanStocksCount = orphanStocks;
    if (orphanStocks > 0) errors.push(`${orphanStocks} stocks have no quote data`);

    await this.checkDataCoverage(reader, stats, warnings, details);

    return { isValid: errors.length === 0, errors, warnings, details };
  }

  async sampleDataset(name: string, query: DatasetSampleQuery): Promise<DatasetSampleResponse | null> {
    const datasetPath = getDatasetPath(name);

    if (!fs.existsSync(datasetPath)) {
      return null;
    }

    const reader = new DatasetReader(datasetPath);
    try {
      const stockList = await reader.getStockList();
      const allCodes = stockList.map((stock) => stock.code);
      const totalAvailable = allCodes.length;
      const sampleSize = Math.min(query.size, totalAvailable);

      const shuffled =
        query.seed !== undefined
          ? this.seededShuffle(allCodes, query.seed)
          : [...allCodes].sort(() => Math.random() - 0.5);
      const sampledCodes = shuffled.slice(0, sampleSize);

      return {
        codes: sampledCodes,
        metadata: {
          totalAvailable,
          sampleSize: sampledCodes.length,
          stratificationUsed: Boolean(query.byMarket || query.bySector),
        },
      };
    } finally {
      await reader.close();
    }
  }

  async searchDataset(name: string, query: DatasetSearchQuery): Promise<DatasetSearchResponse | null> {
    const datasetPath = getDatasetPath(name);

    if (!fs.existsSync(datasetPath)) {
      return null;
    }

    const reader = new DatasetReader(datasetPath);
    try {
      const stockList = await reader.getStockList();
      const results = this.searchStocksInList(stockList, query);

      return {
        results: results.map((r) => transformToSearchResult(r.stock, r.matchType)),
        totalFound: results.length,
      };
    } finally {
      await reader.close();
    }
  }

  private searchStocksInList(
    stockList: StockInfo[],
    query: DatasetSearchQuery
  ): Array<{ stock: StockInfo; matchType: SearchMatchType }> {
    const termLower = query.term.toLowerCase();
    const results: Array<{ stock: StockInfo; matchType: SearchMatchType }> = [];

    for (const stock of stockList) {
      const matchType = query.exact ? matchStockExact(stock, query.term) : matchStockPartial(stock, termLower);

      if (matchType) {
        results.push({ stock, matchType });
        if (results.length >= query.limit) {
          break;
        }
      }
    }

    return results;
  }

  private executeCreateInBackground(
    jobId: string,
    name: string,
    preset: DatasetPreset,
    overwrite: boolean,
    timeoutMinutes?: number
  ): void {
    const timeoutMs = resolveTimeoutMs(timeoutMinutes);
    const job = datasetJobManager.getJob(jobId);
    if (job) {
      job.timeoutId = setTimeout(() => {
        this.handleJobTimeout(jobId, timeoutMs);
      }, timeoutMs);
    }

    const stallCheckInterval = setInterval(() => {
      this.checkForStall(jobId);
    }, STALL_CHECK_INTERVAL_MS);

    setImmediate(async () => {
      try {
        await this.runCreateExecution(jobId, name, preset, overwrite);
      } finally {
        clearInterval(stallCheckInterval);
        const currentJob = datasetJobManager.getJob(jobId);
        if (currentJob?.timeoutId) {
          clearTimeout(currentJob.timeoutId);
        }
      }
    });
  }

  private executeResumeInBackground(jobId: string, name: string, preset: DatasetPreset, timeoutMinutes?: number): void {
    logger.info('Starting dataset resume execution', { jobId, name, preset });

    const timeoutMs = resolveTimeoutMs(timeoutMinutes);
    const timeoutId = setTimeout(() => this.handleJobTimeout(jobId, timeoutMs), timeoutMs);
    const job = datasetJobManager.getJob(jobId);
    if (job) {
      job.timeoutId = timeoutId;
    }

    const stallCheckInterval = setInterval(() => this.checkForStall(jobId), STALL_CHECK_INTERVAL_MS);

    setImmediate(async () => {
      try {
        await this.runResumeExecution(jobId, name, preset);
      } finally {
        clearInterval(stallCheckInterval);
        const currentJob = datasetJobManager.getJob(jobId);
        if (currentJob?.timeoutId) {
          clearTimeout(currentJob.timeoutId);
        }
      }
    });
  }

  private async runResumeExecution(jobId: string, name: string, preset: DatasetPreset): Promise<void> {
    try {
      datasetJobManager.updateStatus(jobId, 'running');

      const client = createJQuantsClient();
      const outputPath = getDatasetPath(name);
      const config = getPresetConfig(preset, outputPath);
      const debugConfig = createDebugConfig({ debug: false });

      const job = datasetJobManager.getJob(jobId);
      const signal = job?.abortController?.signal;

      const builder = new DatasetBuilder(config, client, debugConfig, signal);

      const result = await builder.buildResume((progress: ProgressInfo) => {
        this.handleProgressUpdate(jobId, progress);
      });

      datasetJobManager.completeJob(jobId, {
        success: result.success,
        totalStocks: result.totalStocks,
        processedStocks: result.processedStocks,
        warnings: result.warnings,
        errors: result.errors,
        outputPath,
      });
    } catch (error) {
      this.handleCreateError(jobId, error);
    }
  }

  private handleJobTimeout(jobId: string, timeoutMs = JOB_TIMEOUT_MS): void {
    const job = datasetJobManager.getJob(jobId);
    if (!job) return;

    if (job.status === 'pending' || job.status === 'running') {
      logger.warn('Dataset job timed out', {
        jobId,
        elapsedMs: Date.now() - job.startedAt.getTime(),
        lastProgress: job.progress?.message,
      });

      if (job.abortController) {
        job.abortController.abort();
      }

      datasetJobManager.failJob(jobId, `Job timed out after ${Math.round(timeoutMs / 60000)} minutes`);
    }
  }

  private checkForStall(jobId: string): void {
    const job = datasetJobManager.getJob(jobId);
    if (!job || job.status !== 'running') return;

    const now = Date.now();
    const lastUpdate = job.lastProgressUpdate?.getTime() ?? job.startedAt.getTime();
    const timeSinceUpdate = now - lastUpdate;

    if (timeSinceUpdate > STALL_THRESHOLD_MS) {
      logger.warn('Dataset job may be stalled', {
        jobId,
        timeSinceUpdateMs: timeSinceUpdate,
        lastProgress: job.progress?.message,
        lastStage: job.progress?.stage,
      });
    }
  }

  private async runCreateExecution(
    jobId: string,
    name: string,
    preset: DatasetPreset,
    overwrite: boolean
  ): Promise<void> {
    try {
      datasetJobManager.updateStatus(jobId, 'running');

      const client = createJQuantsClient();
      const outputPath = getDatasetPath(name);
      const config = { ...getPresetConfig(preset, outputPath), overwrite };
      const debugConfig = createDebugConfig({ debug: false });

      const job = datasetJobManager.getJob(jobId);
      const signal = job?.abortController?.signal;
      const builder = new DatasetBuilder(config, client, debugConfig, signal);

      const result = await builder.build((progress: ProgressInfo) => {
        this.handleProgressUpdate(jobId, progress);
      });

      datasetJobManager.completeJob(jobId, {
        success: result.success,
        totalStocks: result.totalStocks,
        processedStocks: result.processedStocks,
        warnings: result.warnings,
        errors: result.errors,
        outputPath,
      });
    } catch (error) {
      this.handleCreateError(jobId, error);
    }
  }

  private handleProgressUpdate(jobId: string, progress: ProgressInfo): void {
    if (datasetJobManager.isJobCancelled(jobId)) {
      throw new Error('Job cancelled');
    }

    const percentage = progress.total > 0 ? Math.round((progress.processed / progress.total) * 100 * 100) / 100 : 0;

    const message = progress.currentItem
      ? `${progress.currentItem} (${progress.processed}/${progress.total})`
      : `${progress.processed}/${progress.total}`;

    datasetJobManager.updateProgress(jobId, {
      stage: progress.stage,
      current: progress.processed,
      total: progress.total,
      percentage,
      message,
    });
  }

  private handleCreateError(jobId: string, error: unknown): void {
    const errorMessage = error instanceof Error ? error.message : String(error);

    if (
      errorMessage === 'Job cancelled' ||
      errorMessage === 'Build cancelled' ||
      errorMessage === 'Operation cancelled'
    ) {
      logger.info('Dataset job was cancelled', { jobId });
      const job = datasetJobManager.getJob(jobId);
      if (job && (job.status === 'pending' || job.status === 'running')) {
        datasetJobManager.cancelJob(jobId);
      }
      return;
    }

    logger.error('Dataset creation failed', { jobId, error: errorMessage });
    datasetJobManager.failJob(jobId, errorMessage);
  }

  private seededShuffle<T>(array: T[], seed: number): T[] {
    const result = [...array];
    let currentSeed = seed;

    const seededRandom = () => {
      const x = Math.sin(currentSeed++) * 10000;
      return x - Math.floor(x);
    };

    for (let i = result.length - 1; i > 0; i--) {
      const j = Math.floor(seededRandom() * (i + 1));
      const temp = result[i];
      const swap = result[j];
      if (temp !== undefined && swap !== undefined) {
        result[i] = swap;
        result[j] = temp;
      }
    }

    return result;
  }
}

// Singleton instance
export const datasetService = new DatasetService();
