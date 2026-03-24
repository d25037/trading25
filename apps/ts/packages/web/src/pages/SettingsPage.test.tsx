import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '@/lib/api-client';
import { SettingsPage } from './SettingsPage';

type SyncJobStatus = {
  jobId: string;
  status: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';
  mode: string;
  progress?: {
    stage: string;
    current: number;
    total: number;
    percentage: number;
    message: string;
  };
  startedAt?: string;
};

const mockStartSyncState = {
  mutate: vi.fn(),
  isPending: false,
  error: null as Error | null,
};

const mockCancelSyncState = {
  mutate: vi.fn(),
  isPending: false,
};

const mockUseSyncJobStatus = vi.fn();
const mockUseSyncFetchDetails = vi.fn();
const mockUseSyncSSE = vi.fn();
const mockUseActiveSyncJob = vi.fn();
const mockUseDbStats = vi.fn();
const mockUseDbValidation = vi.fn();
const mockUseRefreshStocks = vi.fn();

vi.mock('@/hooks/useDbSync', () => ({
  useStartSync: () => mockStartSyncState,
  useCancelSync: () => mockCancelSyncState,
  useActiveSyncJob: () => mockUseActiveSyncJob(),
  useSyncSSE: (jobId: string | null) => mockUseSyncSSE(jobId),
  useSyncJobStatus: (jobId: string | null, sseConnected?: boolean) => mockUseSyncJobStatus(jobId, sseConnected),
  useSyncFetchDetails: (jobId: string | null, sseConnected?: boolean) => mockUseSyncFetchDetails(jobId, sseConnected),
  useDbStats: (options?: unknown) => mockUseDbStats(options),
  useDbValidation: (options?: unknown) => mockUseDbValidation(options),
  useRefreshStocks: () => mockUseRefreshStocks(),
}));

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
  mockStartSyncState.isPending = false;
  mockStartSyncState.error = null;
  mockCancelSyncState.isPending = false;
  mockUseActiveSyncJob.mockReturnValue({
    data: null,
    isLoading: false,
    error: null,
  });
  mockUseSyncSSE.mockReturnValue({
    isConnected: false,
  });
  mockUseSyncFetchDetails.mockReturnValue({
    data: null,
    isLoading: false,
    error: null,
  });
  mockUseRefreshStocks.mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
    error: null,
  });
  mockUseSyncJobStatus.mockImplementation((jobId: string | null) => {
    if (!jobId) {
      return {
        data: null,
        isLoading: false,
        error: null,
      };
    }

    return {
      data: {
        jobId,
        status: 'running',
        mode: 'auto',
        startedAt: '2026-02-13T00:00:00Z',
      } as SyncJobStatus,
      isLoading: false,
      error: null,
    };
  });
  mockUseDbStats.mockReturnValue({
    data: {
      initialized: true,
      lastSync: '2026-02-28T02:29:45.768793+00:00',
      timeSeriesSource: 'duckdb-parquet',
      databaseSize: 4096,
      storage: {
        duckdbBytes: 4096,
        parquetBytes: 8192,
        totalBytes: 12288,
      },
      topix: {
        count: 2450,
        dateRange: { min: '2016-02-29', max: '2026-02-27' },
      },
      stocks: {
        total: 3800,
        byMarket: { Prime: 1800, Standard: 1200, Growth: 800 },
      },
      stockData: {
        count: 1200000,
        dateCount: 520,
        dateRange: { min: '2024-01-01', max: '2026-02-27' },
        averageStocksPerDay: 2307.69,
      },
      indices: {
        masterCount: 75,
        dataCount: 56000,
        dateCount: 520,
        dateRange: { min: '2016-02-29', max: '2026-02-27' },
        byCategory: { sector33: 33, sector17: 17, style: 5 },
      },
      options225: {
        count: 0,
        dateCount: 0,
        dateRange: null,
      },
      margin: {
        count: 2400,
        uniqueStockCount: 1200,
        dateCount: 120,
        dateRange: { min: '2024-01-05', max: '2026-02-27' },
      },
      fundamentals: {
        count: 9800,
        uniqueStockCount: 2500,
        latestDisclosedDate: '2026-02-26',
        listedMarketCoverage: {
          listedMarketStocks: 2500,
          coveredStocks: 2493,
          missingStocks: 7,
          coverageRatio: 0.9972,
          issuerAliasCoveredCount: 6,
          emptySkippedCount: 4,
        },
      },
      lastUpdated: '2026-03-01T12:00:00Z',
    },
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  });
  mockUseDbValidation.mockReturnValue({
    data: {
      status: 'warning',
      initialized: true,
      lastSync: '2026-02-28T02:29:45.768793+00:00',
      lastStocksRefresh: '2026-03-01T03:00:00Z',
      timeSeriesSource: 'duckdb-parquet',
      topix: { count: 2450, dateRange: { min: '2016-02-29', max: '2026-02-27' } },
      stocks: { total: 3800, byMarket: { Prime: 1800 } },
      stockData: {
        count: 1200000,
        dateRange: { min: '2024-01-01', max: '2026-02-27' },
        missingDatesCount: 12,
        missingDates: ['2026-02-27', '2026-02-20'],
      },
      options225: {
        count: 0,
        dateCount: 0,
        dateRange: null,
        missingTopixCoverageDatesCount: 0,
        missingTopixCoverageDates: [],
        missingUnderlyingPriceDatesCount: 0,
        missingUnderlyingPriceDates: [],
        conflictingUnderlyingPriceDatesCount: 0,
        conflictingUnderlyingPriceDates: [],
      },
      margin: {
        count: 2400,
        uniqueStockCount: 1200,
        dateCount: 120,
        dateRange: { min: '2024-01-05', max: '2026-02-27' },
        orphanCount: 3,
        emptySkippedCount: 1,
        emptySkippedCodes: ['4957'],
      },
      fundamentals: {
        count: 9800,
        uniqueStockCount: 2500,
        latestDisclosedDate: '2026-02-26',
        missingListedMarketStocksCount: 7,
        missingListedMarketStocks: ['1301', '9999'],
        issuerAliasCoveredCount: 6,
        emptySkippedCount: 4,
        emptySkippedCodes: ['464A', '500A'],
        failedDatesCount: 0,
        failedCodesCount: 0,
      },
      failedDates: ['2026-02-27', '2026-02-20'],
      failedDatesCount: 3,
      adjustmentEvents: [
        { code: '7203', date: '2026-02-14', adjustmentFactor: 0.5, close: 1000, eventType: 'stock_split' },
      ],
      adjustmentEventsCount: 8,
      stocksNeedingRefresh: ['7203', '6758'],
      stocksNeedingRefreshCount: 100,
      integrityIssues: [{ code: 'chart.stock_data.missing_dates', count: 12 }],
      integrityIssuesCount: 1,
      sampleWindows: {
        stockDataMissingDates: { returnedCount: 2, totalCount: 12, limit: 20, truncated: false },
        failedDates: { returnedCount: 2, totalCount: 3, limit: 10, truncated: false },
        adjustmentEvents: { returnedCount: 1, totalCount: 8, limit: 20, truncated: false },
        stocksNeedingRefresh: { returnedCount: 2, totalCount: 100, limit: 20, truncated: true },
        options225MissingTopixCoverageDates: { returnedCount: 0, totalCount: 0, limit: 20, truncated: false },
        missingListedMarketStocks: { returnedCount: 2, totalCount: 7, limit: 20, truncated: false },
        fundamentalsEmptySkippedCodes: { returnedCount: 2, totalCount: 4, limit: 20, truncated: false },
        marginEmptySkippedCodes: { returnedCount: 1, totalCount: 1, limit: 20, truncated: false },
      },
      recommendations: [
        'Run repair sync to refresh 100 stocks with pending adjustment backfill',
        'Run repair sync to backfill fundamentals for 7 listed-market stocks',
        'Run indices-only sync to ingest N225 options data into options_225_data',
      ],
      lastUpdated: '2026-03-01T12:00:01Z',
    },
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  });
  mockStartSyncState.mutate.mockImplementation((_, options) => {
    options?.onSuccess?.({ jobId: 'job-1', status: 'running', mode: 'auto' });
  });
});

describe('SettingsPage', () => {
  it('loads active job id from localStorage on mount', () => {
    localStorage.setItem('trading25.settings.sync.activeJobId', 'stored-job-1');

    render(<SettingsPage />);

    expect(mockUseSyncJobStatus).toHaveBeenCalledWith('stored-job-1', false);
  });

  it('restores active job id from backend active job endpoint', () => {
    mockUseActiveSyncJob.mockReturnValue({
      data: {
        jobId: 'active-job-1',
        status: 'running',
        mode: 'incremental',
        startedAt: '2026-03-04T00:00:00Z',
      } as SyncJobStatus,
      isLoading: false,
      error: null,
    });

    render(<SettingsPage />);

    expect(mockUseSyncJobStatus).toHaveBeenCalledWith('active-job-1', false);
  });

  it('clears persisted active job id when status endpoint returns 404', async () => {
    localStorage.setItem('trading25.settings.sync.activeJobId', 'missing-job');
    mockUseSyncJobStatus.mockReturnValue({
      data: null,
      isLoading: false,
      error: new ApiError('Job not found', 404),
    });

    render(<SettingsPage />);

    await waitFor(() => {
      expect(localStorage.getItem('trading25.settings.sync.activeJobId')).toBeNull();
    });
  });

  it('starts sync and renders running status', async () => {
    const user = userEvent.setup();

    render(<SettingsPage />);

    await user.click(screen.getByRole('button', { name: /Start Sync/i }));

    expect(mockStartSyncState.mutate).toHaveBeenCalledWith(
      { mode: 'auto', enforceBulkForStockData: false },
      expect.objectContaining({
        onSuccess: expect.any(Function),
      })
    );
    expect(await screen.findByRole('button', { name: /Cancel/i })).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: /Cancel/i }));
    expect(mockCancelSyncState.mutate).toHaveBeenCalledWith('job-1');
  });

  it('passes sync running flag to snapshot hooks', async () => {
    const user = userEvent.setup();

    render(<SettingsPage />);
    expect(mockUseDbStats).toHaveBeenCalledWith({ isSyncRunning: false });
    expect(mockUseDbValidation).toHaveBeenCalledWith({ isSyncRunning: false });

    await user.click(screen.getByRole('button', { name: /Start Sync/i }));

    expect(mockUseDbStats).toHaveBeenCalledWith({ isSyncRunning: true });
    expect(mockUseDbValidation).toHaveBeenCalledWith({ isSyncRunning: true });
  });

  it('starts sync request without legacy sqlite data-plane override', async () => {
    const user = userEvent.setup();

    render(<SettingsPage />);

    await user.click(screen.getByRole('button', { name: /Start Sync/i }));

    expect(mockStartSyncState.mutate).toHaveBeenCalledWith(
      { mode: 'auto', enforceBulkForStockData: false },
      expect.objectContaining({
        onSuccess: expect.any(Function),
      })
    );
  });

  it('shows starting state while request is in progress', () => {
    mockStartSyncState.isPending = true;

    render(<SettingsPage />);

    expect(screen.getByRole('button', { name: /Starting.../i })).toBeInTheDocument();
  });

  it('sends selected sync mode', async () => {
    const user = userEvent.setup();

    render(<SettingsPage />);

    await user.click(screen.getByRole('combobox', { name: 'Sync Mode' }));
    await user.click(screen.getByRole('option', { name: /Incremental Backfill/i }));
    await user.click(screen.getByRole('button', { name: /Start Sync/i }));

    expect(mockStartSyncState.mutate).toHaveBeenCalledWith(
      { mode: 'incremental', enforceBulkForStockData: false },
      expect.objectContaining({
        onSuccess: expect.any(Function),
      })
    );
  });

  it('requires confirmation before reset + initial sync', async () => {
    const user = userEvent.setup();

    render(<SettingsPage />);

    await user.click(screen.getByRole('combobox', { name: 'Sync Mode' }));
    await user.click(screen.getByText(/Full bootstrap of the local DuckDB snapshot/i));
    await user.click(screen.getByRole('switch', { name: /Reset market\.duckdb \+ parquet first/i }));
    await user.click(screen.getByRole('button', { name: /Start Sync/i }));

    expect(mockStartSyncState.mutate).not.toHaveBeenCalled();
    expect(screen.getByRole('heading', { name: /Reset market DB before initial sync\?/i })).toBeInTheDocument();

    await user.type(screen.getByLabelText(/Type RESET to continue/i), 'reset');
    await user.click(screen.getByRole('button', { name: /Reset and Start Sync/i }));

    expect(mockStartSyncState.mutate).toHaveBeenCalledWith(
      { mode: 'initial', enforceBulkForStockData: false, resetBeforeSync: true },
      expect.objectContaining({
        onSuccess: expect.any(Function),
      })
    );
  });

  it('sends enforce bulk option when enabled', async () => {
    const user = userEvent.setup();

    render(<SettingsPage />);

    await user.click(screen.getByRole('switch', { name: /Enforce BULK for stock_data/i }));
    await user.click(screen.getByRole('button', { name: /Start Sync/i }));

    expect(mockStartSyncState.mutate).toHaveBeenCalledWith(
      { mode: 'auto', enforceBulkForStockData: true },
      expect.objectContaining({
        onSuccess: expect.any(Function),
      })
    );
  });

  it('shows sync error message when start sync fails', () => {
    mockStartSyncState.error = new Error('Sync failed');

    render(<SettingsPage />);

    expect(screen.getByText('Sync failed')).toBeInTheDocument();
  });

  it('shows bulk/rest fetch details while sync is running', async () => {
    const user = userEvent.setup();
    mockUseSyncJobStatus.mockImplementation((jobId: string | null) => {
      if (!jobId) {
        return {
          data: null,
          isLoading: false,
          error: null,
        };
      }
      return {
        data: {
          jobId,
          status: 'running',
          mode: 'incremental',
          startedAt: '2026-02-13T00:00:00Z',
          progress: {
            stage: 'stock_data',
            current: 2,
            total: 5,
            percentage: 40,
            message: 'Fetch strategy: /equities/bars/daily -> BULK (REST est=120, BULK est=6, targets=42 dates)',
          },
        } as SyncJobStatus,
        isLoading: false,
        error: null,
      };
    });

    render(<SettingsPage />);

    await user.click(screen.getByRole('button', { name: /Start Sync/i }));

    expect(await screen.findByText('Fetch')).toBeInTheDocument();
    expect(screen.getAllByText('BULK').length).toBeGreaterThan(0);
    expect(screen.getAllByText('/equities/bars/daily').length).toBeGreaterThan(0);
  });

  it('renders market db snapshot from stats/validate', () => {
    render(<SettingsPage />);

    expect(screen.getByRole('heading', { name: 'Market DB' })).toBeInTheDocument();
    expect(screen.getByText('DuckDB Snapshot')).toBeInTheDocument();
    expect(screen.getByText('Snapshot Summary')).toBeInTheDocument();
    expect(screen.getByText('Data Coverage')).toBeInTheDocument();
    expect(screen.getByText('Validation Diagnostics')).toBeInTheDocument();
    expect(screen.getByText('Actionable Warnings')).toBeInTheDocument();
    expect(screen.getByText('Informational Diagnostics')).toBeInTheDocument();
    expect(screen.getByText('Last Stock Refresh')).toBeInTheDocument();
    expect(screen.getByText('Local Storage')).toBeInTheDocument();
    expect(screen.getAllByText('12 KB').length).toBeGreaterThan(0);
    expect(screen.getByText('Stock Data')).toBeInTheDocument();
    expect(screen.getByText('Fundamentals')).toBeInTheDocument();
    expect(screen.getAllByText('N225 Options').length).toBeGreaterThan(0);
    expect(screen.getByText('Not ingested')).toBeInTheDocument();
    expect(screen.getByText('Status: No local options chain yet (TOPIX latest 2026-02-27)')).toBeInTheDocument();
    expect(screen.getByText('Alias covered: 6')).toBeInTheDocument();
    expect(screen.getByText('N225 Options Missing Locally')).toBeInTheDocument();
    expect(screen.getByText('Margin Orphans')).toBeInTheDocument();
    expect(screen.getByText('Missing Stock Dates')).toBeInTheDocument();
    expect(screen.getByText('12')).toBeInTheDocument();
    expect(screen.getAllByText('Sample dates: 2026-02-27, 2026-02-20').length).toBeGreaterThan(0);
    expect(screen.getByText('Failed Sync Dates')).toBeInTheDocument();
    expect(screen.getByText('Adjustment Events')).toBeInTheDocument();
    expect(screen.getByText('Stocks Needing Refresh')).toBeInTheDocument();
    expect(screen.getByText('Showing 2 of 100.')).toBeInTheDocument();
    expect(screen.getByText('Readiness Issues')).toBeInTheDocument();
    expect(screen.getAllByText('Missing Listed-Market Fundamentals').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Unsupported/Empty Fundamentals').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Unsupported/Empty Margin Codes').length).toBeGreaterThan(0);
    expect(screen.getByText('Sample codes: 1301, 9999')).toBeInTheDocument();
    expect(screen.getByText('Sample codes: 464A, 500A')).toBeInTheDocument();
    expect(screen.getByText('Sample codes: 4957')).toBeInTheDocument();
    expect(screen.getByText('Warning Details')).toBeInTheDocument();
    expect(
      screen.getAllByText('Run repair sync to refresh 100 stocks with pending adjustment backfill').length
    ).toBeGreaterThan(0);
    expect(
      screen.getAllByText('Run repair sync to backfill fundamentals for 7 listed-market stocks').length
    ).toBeGreaterThan(0);
    expect(
      screen.getByText('Run indices-only sync to ingest N225 options data into options_225_data')
    ).toBeInTheDocument();
    expect(screen.getByText('Warning Recovery')).toBeInTheDocument();
    expect(screen.getByText('Repair Warnings')).toBeInTheDocument();
  });

  it('renders stale N225 options as an actionable warning instead of a silent date', () => {
    mockUseDbStats.mockReturnValue({
      data: {
        initialized: true,
        lastSync: '2026-02-28T02:29:45.768793+00:00',
        timeSeriesSource: 'duckdb-parquet',
        databaseSize: 4096,
        storage: {
          duckdbBytes: 4096,
          parquetBytes: 8192,
          totalBytes: 12288,
        },
        topix: {
          count: 2450,
          dateRange: { min: '2016-02-29', max: '2026-02-27' },
        },
        stocks: {
          total: 3800,
          byMarket: { Prime: 1800, Standard: 1200, Growth: 800 },
        },
        stockData: {
          count: 1200000,
          dateCount: 520,
          dateRange: { min: '2024-01-01', max: '2026-02-27' },
          averageStocksPerDay: 2307.69,
        },
        indices: {
          masterCount: 75,
          dataCount: 56000,
          dateCount: 520,
          dateRange: { min: '2016-02-29', max: '2026-02-27' },
          byCategory: { sector33: 33, sector17: 17, style: 5 },
        },
        options225: {
          count: 12,
          dateCount: 2,
          dateRange: { min: '2026-02-20', max: '2026-02-20' },
        },
        margin: {
          count: 2400,
          uniqueStockCount: 1200,
          dateCount: 120,
          dateRange: { min: '2024-01-05', max: '2026-02-27' },
        },
        fundamentals: {
          count: 9800,
          uniqueStockCount: 2500,
          latestDisclosedDate: '2026-02-26',
          listedMarketCoverage: {
            listedMarketStocks: 2500,
            coveredStocks: 2499,
            missingStocks: 1,
            coverageRatio: 0.9996,
            issuerAliasCoveredCount: 0,
            emptySkippedCount: 0,
          },
        },
        lastUpdated: '2026-03-01T12:00:00Z',
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });
    mockUseDbValidation.mockReturnValue({
      data: {
        status: 'warning',
        initialized: true,
        lastSync: '2026-02-28T02:29:45.768793+00:00',
        lastStocksRefresh: '2026-03-01T03:00:00Z',
        timeSeriesSource: 'duckdb-parquet',
        topix: { count: 2450, dateRange: { min: '2016-02-29', max: '2026-02-27' } },
        stocks: { total: 3800, byMarket: { Prime: 1800 } },
        stockData: {
          count: 1200000,
          dateRange: { min: '2024-01-01', max: '2026-02-27' },
          missingDatesCount: 0,
          missingDates: [],
        },
        options225: {
          count: 12,
          dateCount: 2,
          dateRange: { min: '2026-02-20', max: '2026-02-20' },
          missingTopixCoverageDatesCount: 0,
          missingTopixCoverageDates: [],
          missingUnderlyingPriceDatesCount: 0,
          missingUnderlyingPriceDates: [],
          conflictingUnderlyingPriceDatesCount: 0,
          conflictingUnderlyingPriceDates: [],
        },
        margin: {
          count: 2400,
          uniqueStockCount: 1200,
          dateCount: 120,
          dateRange: { min: '2024-01-05', max: '2026-02-27' },
          orphanCount: 0,
          emptySkippedCount: 0,
          emptySkippedCodes: [],
        },
        fundamentals: {
          count: 9800,
          uniqueStockCount: 2500,
          latestDisclosedDate: '2026-02-26',
          missingListedMarketStocksCount: 1,
          missingListedMarketStocks: ['1301'],
          issuerAliasCoveredCount: 0,
          emptySkippedCount: 0,
          emptySkippedCodes: [],
          failedDatesCount: 0,
          failedCodesCount: 0,
        },
        failedDates: [],
        failedDatesCount: 0,
        adjustmentEvents: [],
        adjustmentEventsCount: 0,
        stocksNeedingRefresh: [],
        stocksNeedingRefreshCount: 0,
        integrityIssues: [],
        integrityIssuesCount: 0,
        sampleWindows: {
          stockDataMissingDates: { returnedCount: 0, totalCount: 0, limit: 20, truncated: false },
          failedDates: { returnedCount: 0, totalCount: 0, limit: 10, truncated: false },
          adjustmentEvents: { returnedCount: 0, totalCount: 0, limit: 20, truncated: false },
          stocksNeedingRefresh: { returnedCount: 0, totalCount: 0, limit: 20, truncated: false },
          options225MissingTopixCoverageDates: { returnedCount: 0, totalCount: 0, limit: 20, truncated: false },
          options225MissingUnderlyingPriceDates: { returnedCount: 0, totalCount: 0, limit: 20, truncated: false },
          options225ConflictingUnderlyingPriceDates: { returnedCount: 0, totalCount: 0, limit: 20, truncated: false },
          missingListedMarketStocks: { returnedCount: 1, totalCount: 1, limit: 20, truncated: false },
          fundamentalsEmptySkippedCodes: { returnedCount: 0, totalCount: 0, limit: 20, truncated: false },
          marginEmptySkippedCodes: { returnedCount: 0, totalCount: 0, limit: 20, truncated: false },
        },
        recommendations: [
          'Run indices-only sync to refresh N225 options data through 2026-02-27 (latest local options date: 2026-02-20)',
        ],
        lastUpdated: '2026-03-01T12:00:01Z',
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    render(<SettingsPage />);

    expect(screen.getByText('N225 Options Stale')).toBeInTheDocument();
    expect(screen.getByText('2026-02-20 (stale)')).toBeInTheDocument();
    expect(screen.getByText('Status: Behind TOPIX latest 2026-02-27')).toBeInTheDocument();
    expect(
      screen.getAllByText(
        'Run indices-only sync to refresh N225 options data through 2026-02-27 (latest local options date: 2026-02-20)'
      ).length
    ).toBeGreaterThan(0);
  });

  it('does not count N225 options sync gaps as repair targets', () => {
    mockUseDbValidation.mockReturnValue({
      data: {
        status: 'warning',
        initialized: true,
        topix: { count: 2450, dateRange: { min: '2016-02-29', max: '2026-02-27' } },
        stockData: { missingDatesCount: 0, missingDates: [] },
        options225: {
          count: 0,
          dateCount: 0,
          dateRange: null,
          missingTopixCoverageDatesCount: 0,
          missingTopixCoverageDates: [],
          missingUnderlyingPriceDatesCount: 0,
          missingUnderlyingPriceDates: [],
          conflictingUnderlyingPriceDatesCount: 0,
          conflictingUnderlyingPriceDates: [],
        },
        margin: {
          count: 0,
          uniqueStockCount: 0,
          dateCount: 0,
          dateRange: null,
          orphanCount: 0,
          emptySkippedCount: 0,
          emptySkippedCodes: [],
        },
        fundamentals: {
          missingListedMarketStocksCount: 0,
          missingListedMarketStocks: [],
          issuerAliasCoveredCount: 0,
          emptySkippedCount: 0,
          emptySkippedCodes: [],
          failedDatesCount: 0,
          failedCodesCount: 0,
        },
        failedDatesCount: 0,
        stocksNeedingRefreshCount: 0,
        integrityIssuesCount: 0,
        recommendations: ['Run indices-only sync to ingest N225 options data into options_225_data'],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    render(<SettingsPage />);

    expect(screen.getByText('N225 Options Missing Locally')).toBeInTheDocument();
    expect(screen.getByText('Repair signals')).toBeInTheDocument();
    expect(screen.getByText('No Repairs Needed')).toBeInTheDocument();
  });

  it('renders partial N225 options coverage as a warning instead of in-sync status', () => {
    mockUseDbStats.mockReturnValue({
      data: {
        initialized: true,
        lastSync: '2026-03-18T02:29:45.768793+00:00',
        timeSeriesSource: 'duckdb-parquet',
        databaseSize: 4096,
        storage: {
          duckdbBytes: 4096,
          parquetBytes: 8192,
          totalBytes: 12288,
        },
        topix: {
          count: 2455,
          dateRange: { min: '2016-02-29', max: '2026-03-18' },
        },
        stocks: {
          total: 3800,
          byMarket: { Prime: 1800, Standard: 1200, Growth: 800 },
        },
        stockData: {
          count: 1200000,
          dateCount: 521,
          dateRange: { min: '2024-01-01', max: '2026-03-18' },
          averageStocksPerDay: 2303.26,
        },
        indices: {
          masterCount: 75,
          dataCount: 56000,
          dateCount: 521,
          dateRange: { min: '2016-02-29', max: '2026-03-18' },
          byCategory: { sector33: 33, sector17: 17, style: 5 },
        },
        options225: {
          count: 8354,
          dateCount: 1,
          dateRange: { min: '2026-03-18', max: '2026-03-18' },
        },
        margin: {
          count: 2400,
          uniqueStockCount: 1200,
          dateCount: 120,
          dateRange: { min: '2024-01-05', max: '2026-03-18' },
        },
        fundamentals: {
          count: 9800,
          uniqueStockCount: 2500,
          latestDisclosedDate: '2026-03-18',
          listedMarketCoverage: {
            listedMarketStocks: 2500,
            coveredStocks: 2499,
            missingStocks: 1,
            coverageRatio: 0.9996,
            issuerAliasCoveredCount: 0,
            emptySkippedCount: 0,
          },
        },
        lastUpdated: '2026-03-19T12:00:00Z',
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });
    mockUseDbValidation.mockReturnValue({
      data: {
        status: 'warning',
        initialized: true,
        lastSync: '2026-03-18T02:29:45.768793+00:00',
        lastStocksRefresh: '2026-03-19T03:00:00Z',
        timeSeriesSource: 'duckdb-parquet',
        topix: { count: 2455, dateRange: { min: '2016-02-29', max: '2026-03-18' } },
        stocks: { total: 3800, byMarket: { Prime: 1800 } },
        stockData: {
          count: 1200000,
          dateRange: { min: '2024-01-01', max: '2026-03-18' },
          missingDatesCount: 0,
          missingDates: [],
        },
        options225: {
          count: 8354,
          dateCount: 1,
          dateRange: { min: '2026-03-18', max: '2026-03-18' },
          missingTopixCoverageDatesCount: 2454,
          missingTopixCoverageDates: ['2026-03-17', '2026-03-14'],
          missingUnderlyingPriceDatesCount: 0,
          missingUnderlyingPriceDates: [],
          conflictingUnderlyingPriceDatesCount: 0,
          conflictingUnderlyingPriceDates: [],
        },
        margin: {
          count: 2400,
          uniqueStockCount: 1200,
          dateCount: 120,
          dateRange: { min: '2024-01-05', max: '2026-03-18' },
          orphanCount: 0,
          emptySkippedCount: 0,
          emptySkippedCodes: [],
        },
        fundamentals: {
          count: 9800,
          uniqueStockCount: 2500,
          latestDisclosedDate: '2026-03-18',
          missingListedMarketStocksCount: 1,
          missingListedMarketStocks: ['1301'],
          issuerAliasCoveredCount: 0,
          emptySkippedCount: 0,
          emptySkippedCodes: [],
          failedDatesCount: 0,
          failedCodesCount: 0,
        },
        failedDates: [],
        failedDatesCount: 0,
        adjustmentEvents: [],
        adjustmentEventsCount: 0,
        stocksNeedingRefresh: [],
        stocksNeedingRefreshCount: 0,
        integrityIssues: [],
        integrityIssuesCount: 0,
        sampleWindows: {
          stockDataMissingDates: { returnedCount: 0, totalCount: 0, limit: 20, truncated: false },
          failedDates: { returnedCount: 0, totalCount: 0, limit: 10, truncated: false },
          adjustmentEvents: { returnedCount: 0, totalCount: 0, limit: 20, truncated: false },
          stocksNeedingRefresh: { returnedCount: 0, totalCount: 0, limit: 20, truncated: false },
          options225MissingTopixCoverageDates: { returnedCount: 2, totalCount: 2454, limit: 20, truncated: true },
          options225MissingUnderlyingPriceDates: { returnedCount: 0, totalCount: 0, limit: 20, truncated: false },
          options225ConflictingUnderlyingPriceDates: { returnedCount: 0, totalCount: 0, limit: 20, truncated: false },
          missingListedMarketStocks: { returnedCount: 1, totalCount: 1, limit: 20, truncated: false },
          fundamentalsEmptySkippedCodes: { returnedCount: 0, totalCount: 0, limit: 20, truncated: false },
          marginEmptySkippedCodes: { returnedCount: 0, totalCount: 0, limit: 20, truncated: false },
        },
        recommendations: [
          'Run indices-only sync to backfill N225 options history for 2454 TOPIX dates missing from options_225_data',
        ],
        lastUpdated: '2026-03-19T12:00:01Z',
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    render(<SettingsPage />);

    expect(screen.getByText('N225 Options Partial Coverage')).toBeInTheDocument();
    expect(screen.getByText('2026-03-18 (partial)')).toBeInTheDocument();
    expect(screen.getByText('Status: Missing local coverage for 2,454 TOPIX dates')).toBeInTheDocument();
    expect(screen.getByText('Sample dates: 2026-03-17, 2026-03-14')).toBeInTheDocument();
    expect(screen.getByText('Showing 2 of 2,454.')).toBeInTheDocument();
    expect(
      screen.getAllByText(
        'Run indices-only sync to backfill N225 options history for 2454 TOPIX dates missing from options_225_data'
      ).length
    ).toBeGreaterThan(0);
  });

  it('renders manual stock refresh controls alongside snapshot info', () => {
    render(<SettingsPage />);

    expect(screen.getByRole('button', { name: /Refresh Stocks/i })).toBeInTheDocument();
    expect(screen.getByPlaceholderText('e.g. 7203, 6758, 9984')).toBeInTheDocument();
    expect(screen.getByText('Last Stock Refresh')).toBeInTheDocument();
  });

  it('validates stock refresh input before submit', async () => {
    const user = userEvent.setup();
    const mutate = vi.fn();
    mockUseRefreshStocks.mockReturnValue({
      mutate,
      isPending: false,
      error: null,
    });

    render(<SettingsPage />);

    await user.type(screen.getByPlaceholderText('e.g. 7203, 6758, 9984'), 'abc,12,12345');
    await user.click(screen.getByRole('button', { name: /Refresh Stocks/i }));

    expect(screen.getByText(/Enter at least one 4-digit stock code/i)).toBeInTheDocument();
    expect(mutate).not.toHaveBeenCalled();
  });

  it('submits refresh request and renders refresh result', async () => {
    const user = userEvent.setup();
    const mutate = vi.fn((_request, options) => {
      options?.onSuccess?.({
        totalStocks: 2,
        successCount: 2,
        failedCount: 0,
        totalApiCalls: 2,
        totalRecordsStored: 120,
        results: [
          { code: '7203', success: true, recordsFetched: 60, recordsStored: 60 },
          { code: '6758', success: true, recordsFetched: 60, recordsStored: 60 },
        ],
        errors: [],
        lastUpdated: '2026-03-03T00:00:00Z',
      });
    });

    mockUseRefreshStocks.mockReturnValue({
      mutate,
      isPending: false,
      error: null,
    });

    render(<SettingsPage />);

    await user.type(screen.getByPlaceholderText('e.g. 7203, 6758, 9984'), '7203, 6758, 7203');
    await user.click(screen.getByRole('button', { name: /Refresh Stocks/i }));

    expect(mutate).toHaveBeenCalledWith(
      { codes: ['7203', '6758'] },
      expect.objectContaining({ onSuccess: expect.any(Function) })
    );
    expect(await screen.findByText('Total Stocks')).toBeInTheDocument();
    expect(screen.getByText('120')).toBeInTheDocument();
    expect(screen.getByText('7203')).toBeInTheDocument();
    expect(screen.getByText('6758')).toBeInTheDocument();
  });

  it('rejects refresh request when more than 50 codes are provided', async () => {
    const user = userEvent.setup();
    const mutate = vi.fn();
    mockUseRefreshStocks.mockReturnValue({
      mutate,
      isPending: false,
      error: null,
    });

    render(<SettingsPage />);

    const codes = Array.from({ length: 51 }, (_, index) => `${(1000 + index).toString().padStart(4, '0')}`).join(',');
    fireEvent.change(screen.getByPlaceholderText('e.g. 7203, 6758, 9984'), { target: { value: codes } });
    await user.click(screen.getByRole('button', { name: /Refresh Stocks/i }));

    expect(screen.getByText(/Maximum 50 stock codes are allowed/i)).toBeInTheDocument();
    expect(mutate).not.toHaveBeenCalled();
  });

  it('starts repair sync from warning recovery card', async () => {
    const user = userEvent.setup();

    render(<SettingsPage />);

    await user.click(screen.getByRole('button', { name: /Repair Warnings/i }));

    expect(mockStartSyncState.mutate).toHaveBeenCalledWith(
      { mode: 'repair', enforceBulkForStockData: false },
      expect.objectContaining({
        onSuccess: expect.any(Function),
      })
    );
  });

  it('renders warning recovery safely when fundamentals counts are zero', () => {
    mockUseDbValidation.mockReturnValue({
      data: {
        status: 'warning',
        stockData: { missingDatesCount: 2 },
        margin: {
          count: 0,
          uniqueStockCount: 0,
          dateCount: 0,
          dateRange: null,
          orphanCount: 0,
          emptySkippedCount: 0,
          emptySkippedCodes: [],
        },
        fundamentals: {
          missingListedMarketStocksCount: 0,
          missingListedMarketStocks: [],
          issuerAliasCoveredCount: 0,
          emptySkippedCount: 0,
          emptySkippedCodes: [],
          failedDatesCount: 0,
          failedCodesCount: 0,
        },
        failedDatesCount: 0,
        stocksNeedingRefreshCount: 3,
        integrityIssuesCount: 0,
        recommendations: ['Run repair sync to refresh 3 stocks with pending adjustment backfill'],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    render(<SettingsPage />);

    expect(screen.getByText('Warning Recovery')).toBeInTheDocument();
    expect(screen.getByText('Stocks needing refresh')).toBeInTheDocument();
    expect(screen.getByText('Missing listed-market fundamentals')).toBeInTheDocument();
    expect(screen.getAllByText('0').length).toBeGreaterThan(0);
  });

  it('shows validation notes when status is healthy and recommendations exist', () => {
    mockUseDbValidation.mockReturnValue({
      data: {
        status: 'healthy',
        stockData: { missingDatesCount: 0 },
        margin: {
          count: 0,
          uniqueStockCount: 0,
          dateCount: 0,
          dateRange: null,
          orphanCount: 0,
          emptySkippedCount: 0,
          emptySkippedCodes: [],
        },
        fundamentals: {
          missingListedMarketStocksCount: 0,
          missingListedMarketStocks: [],
          issuerAliasCoveredCount: 0,
          emptySkippedCount: 0,
          emptySkippedCodes: [],
          failedDatesCount: 0,
          failedCodesCount: 0,
        },
        failedDatesCount: 0,
        stocksNeedingRefreshCount: 0,
        integrityIssuesCount: 0,
        recommendations: ['Backtest signal readiness: unmet requirements (margin)'],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    render(<SettingsPage />);

    expect(screen.getByText('Validation Notes')).toBeInTheDocument();
    expect(screen.getAllByText('Backtest signal readiness: unmet requirements (margin)').length).toBeGreaterThan(0);
  });

  it('shows error details when validation status is error', () => {
    mockUseDbValidation.mockReturnValue({
      data: {
        status: 'error',
        stockData: { missingDatesCount: 0 },
        margin: {
          count: 0,
          uniqueStockCount: 0,
          dateCount: 0,
          dateRange: null,
          orphanCount: 0,
          emptySkippedCount: 0,
          emptySkippedCodes: [],
        },
        fundamentals: {
          missingListedMarketStocksCount: 0,
          missingListedMarketStocks: [],
          issuerAliasCoveredCount: 0,
          emptySkippedCount: 0,
          emptySkippedCodes: [],
          failedDatesCount: 0,
          failedCodesCount: 0,
        },
        failedDatesCount: 0,
        stocksNeedingRefreshCount: 0,
        integrityIssues: [{ code: 'chart.topix_data.missing', count: 1 }],
        integrityIssuesCount: 1,
        recommendations: ['Run initial sync to populate the database'],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    render(<SettingsPage />);

    expect(screen.getByText('Error Details')).toBeInTheDocument();
    expect(screen.getAllByText('Run initial sync to populate the database').length).toBeGreaterThan(0);
  });

  it('hides validation detail panel when no recommendations are returned', () => {
    mockUseDbValidation.mockReturnValue({
      data: {
        status: 'warning',
        stockData: { missingDatesCount: 1 },
        margin: {
          count: 0,
          uniqueStockCount: 0,
          dateCount: 0,
          dateRange: null,
          orphanCount: 0,
          emptySkippedCount: 0,
          emptySkippedCodes: [],
        },
        fundamentals: {
          missingListedMarketStocksCount: 0,
          missingListedMarketStocks: [],
          issuerAliasCoveredCount: 0,
          emptySkippedCount: 0,
          emptySkippedCodes: [],
          failedDatesCount: 0,
          failedCodesCount: 0,
        },
        failedDatesCount: 0,
        stocksNeedingRefreshCount: 0,
        integrityIssuesCount: 0,
        recommendations: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    render(<SettingsPage />);

    expect(screen.queryByText('Warning Details')).not.toBeInTheDocument();
    expect(screen.queryByText('Validation Notes')).not.toBeInTheDocument();
    expect(screen.queryByText('Error Details')).not.toBeInTheDocument();
  });

  it('shows loading message while db stats and validation are loading', () => {
    mockUseDbStats.mockReturnValue({
      data: null,
      isLoading: true,
      error: null,
      refetch: vi.fn(),
    });
    mockUseDbValidation.mockReturnValue({
      data: null,
      isLoading: true,
      error: null,
      refetch: vi.fn(),
    });

    render(<SettingsPage />);

    expect(screen.getByText('Loading market DB status...')).toBeInTheDocument();
  });

  it('shows market db status error message', () => {
    mockUseDbStats.mockReturnValue({
      data: null,
      isLoading: false,
      error: new Error('Failed to load db stats'),
      refetch: vi.fn(),
    });
    mockUseDbValidation.mockReturnValue({
      data: null,
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    render(<SettingsPage />);

    expect(screen.getByText('Failed to load db stats')).toBeInTheDocument();
  });

  it('keeps validation diagnostics visible when stats request fails', () => {
    mockUseDbStats.mockReturnValue({
      data: null,
      isLoading: false,
      error: new Error('Failed to load db stats'),
      refetch: vi.fn(),
    });

    render(<SettingsPage />);

    expect(screen.getByText('Failed to load db stats')).toBeInTheDocument();
    expect(screen.getByText('Validation Diagnostics')).toBeInTheDocument();
    expect(screen.getByText('Missing Stock Dates')).toBeInTheDocument();
    expect(screen.getByText('Preferred Alias Covered')).toBeInTheDocument();
  });

  it('shows refresh pending state and refresh error', () => {
    mockUseRefreshStocks.mockReturnValue({
      mutate: vi.fn(),
      isPending: true,
      error: new Error('Refresh failed'),
    });

    render(<SettingsPage />);

    expect(screen.getByRole('button', { name: /Refreshing.../i })).toBeInTheDocument();
    expect(screen.getByText('Refresh failed')).toBeInTheDocument();
  });

  it('renders failed refresh row and keeps raw timestamp when invalid', async () => {
    const user = userEvent.setup();
    const mutate = vi.fn((_request, options) => {
      options?.onSuccess?.({
        totalStocks: 1,
        successCount: 0,
        failedCount: 1,
        totalApiCalls: 1,
        totalRecordsStored: 0,
        results: [{ code: '7203', success: false, recordsFetched: 0, recordsStored: 0, error: 'api error' }],
        errors: ['api error'],
        lastUpdated: 'not-a-date',
      });
    });
    mockUseRefreshStocks.mockReturnValue({
      mutate,
      isPending: false,
      error: null,
    });

    render(<SettingsPage />);

    fireEvent.change(screen.getByPlaceholderText('e.g. 7203, 6758, 9984'), { target: { value: '7203' } });
    await user.click(screen.getByRole('button', { name: /Refresh Stocks/i }));

    expect(await screen.findByRole('cell', { name: 'failed' })).toBeInTheDocument();
    expect(await screen.findByText('api error')).toBeInTheDocument();
    expect(screen.getByText('not-a-date')).toBeInTheDocument();
  });
});
