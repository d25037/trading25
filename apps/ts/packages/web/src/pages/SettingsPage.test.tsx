import { render, screen, waitFor } from '@testing-library/react';
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
const mockUseActiveSyncJob = vi.fn();
const mockUseDbStats = vi.fn();
const mockUseDbValidation = vi.fn();
const mockUseRefreshStocks = vi.fn();

vi.mock('@/hooks/useDbSync', () => ({
  useStartSync: () => mockStartSyncState,
  useCancelSync: () => mockCancelSyncState,
  useActiveSyncJob: () => mockUseActiveSyncJob(),
  useSyncJobStatus: (jobId: string | null) => mockUseSyncJobStatus(jobId),
  useSyncFetchDetails: (jobId: string | null) => mockUseSyncFetchDetails(jobId),
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
      stockData: { dateRange: { min: '2024-01-01', max: '2026-02-27' } },
      topix: { dateRange: { min: '2016-02-29', max: '2026-02-27' } },
      indices: { dateRange: { min: '2016-02-29', max: '2026-02-27' } },
      margin: {
        count: 2400,
        uniqueStockCount: 1200,
        dateCount: 120,
        dateRange: { min: '2024-01-05', max: '2026-02-27' },
      },
    },
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  });
  mockUseDbValidation.mockReturnValue({
    data: {
      status: 'warning',
      stockData: { missingDatesCount: 12 },
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
        missingListedMarketStocksCount: 7,
        missingListedMarketStocks: ['1301', '9999'],
        issuerAliasCoveredCount: 6,
        emptySkippedCount: 4,
        emptySkippedCodes: ['464A', '500A'],
        failedDatesCount: 0,
        failedCodesCount: 0,
      },
      failedDatesCount: 3,
      stocksNeedingRefreshCount: 100,
      integrityIssuesCount: 0,
      recommendations: [
        'Run repair sync to refresh 100 stocks with pending adjustment backfill',
        'Run repair sync to backfill fundamentals for 7 listed-market stocks',
      ],
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

    expect(mockUseSyncJobStatus).toHaveBeenCalledWith('stored-job-1');
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

    expect(mockUseSyncJobStatus).toHaveBeenCalledWith('active-job-1');
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
    expect(screen.getByText('Stock Data Latest')).toBeInTheDocument();
    expect(screen.getAllByText('2026-02-27').length).toBeGreaterThan(0);
    expect(screen.getByText('Margin Latest')).toBeInTheDocument();
    expect(screen.getByText('Margin Stocks')).toBeInTheDocument();
    expect(screen.getByText('Margin Orphans')).toBeInTheDocument();
    expect(screen.getByText('Missing Stock Dates')).toBeInTheDocument();
    expect(screen.getByText('12')).toBeInTheDocument();
    expect(screen.getByText('Stocks Needing Refresh')).toBeInTheDocument();
    expect(screen.getAllByText('Missing Listed-Market Fundamentals').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Unsupported/Empty Fundamentals').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Preferred Alias Covered').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Unsupported/Empty Margin Codes').length).toBeGreaterThan(0);
    expect(screen.getByText('Coverage Diagnostics')).toBeInTheDocument();
    expect(screen.getByText('Sample codes: 1301, 9999')).toBeInTheDocument();
    expect(screen.getByText('Sample codes: 464A, 500A')).toBeInTheDocument();
    expect(screen.getByText('Sample codes: 4957')).toBeInTheDocument();
    expect(screen.getByText('Warning Details')).toBeInTheDocument();
    expect(
      screen.getAllByText('Run repair sync to refresh 100 stocks with pending adjustment backfill').length
    ).toBeGreaterThan(0);
    expect(screen.getAllByText('Run repair sync to backfill fundamentals for 7 listed-market stocks').length).toBeGreaterThan(0);
    expect(screen.getByText('Warning Recovery')).toBeInTheDocument();
    expect(screen.getByText('Repair Warnings')).toBeInTheDocument();
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
    await user.type(screen.getByPlaceholderText('e.g. 7203, 6758, 9984'), codes);
    await user.click(screen.getByRole('button', { name: /Refresh Stocks/i }));

    expect(screen.getByText(/Maximum 50 stock codes are allowed/i)).toBeInTheDocument();
    expect(mutate).not.toHaveBeenCalled();
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

    await user.type(screen.getByPlaceholderText('e.g. 7203, 6758, 9984'), '7203');
    await user.click(screen.getByRole('button', { name: /Refresh Stocks/i }));

    expect(await screen.findByText('failed')).toBeInTheDocument();
    expect(screen.getByText('api error')).toBeInTheDocument();
    expect(screen.getByText('not-a-date')).toBeInTheDocument();
  });
});
