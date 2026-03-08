import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '@/lib/api-client';
import { createInitialAnalysisState, useAnalysisStore } from '@/stores/analysisStore';
import type { MarketScreeningResponse, ScreeningJobResponse } from '@/types/screening';
import { AnalysisPage } from './AnalysisPage';

const mockNavigate = vi.fn();

const mockChartStore = {
  setSelectedSymbol: vi.fn(),
};

const mockScreeningFilters = vi.fn((_props: unknown) => <div>Screening Filters</div>);
const mockScreeningTable = vi.fn(({ onStockClick }: { onStockClick: (code: string) => void }) => (
  <button type="button" onClick={() => onStockClick('7203')}>
    Screening Row
  </button>
));
const mockRunScreeningJob = vi.fn().mockResolvedValue({
  job_id: 'job-1',
  status: 'pending',
});
const mockUseScreeningJobStatus = vi.fn();
const mockUseScreeningResult = vi.fn();
const mockCancelScreeningJob = vi.fn();
let mockStrategiesQueryResult: {
  data: {
    strategies: Array<{ name: string; category: string; screening_mode: 'standard' | 'oracle' | 'unsupported' }>;
  } | null;
  isLoading: boolean;
  error: Error | null;
};

function createCachedScreeningResult(): MarketScreeningResponse {
  return {
    mode: 'standard',
    summary: {
      totalStocksScreened: 1,
      matchCount: 1,
      skippedCount: 0,
      byStrategy: { 'production/range_break_v15': 1 },
      strategiesEvaluated: ['production/range_break_v15'],
      strategiesWithoutBacktestMetrics: [],
      warnings: [],
    },
    markets: ['prime'],
    recentDays: 10,
    referenceDate: '2026-02-18',
    sortBy: 'matchedDate',
    order: 'desc',
    lastUpdated: '2026-02-18T00:00:00Z',
    results: [
      {
        stockCode: '7203',
        companyName: 'トヨタ自動車',
        matchedDate: '2026-02-18',
        bestStrategyName: 'production/range_break_v15',
        bestStrategyScore: 1.1,
        matchStrategyCount: 1,
        matchedStrategies: [
          {
            strategyName: 'production/range_break_v15',
            matchedDate: '2026-02-18',
            strategyScore: 1.1,
          },
        ],
      },
    ],
  };
}

function createScreeningJob(overrides: Partial<ScreeningJobResponse> = {}): ScreeningJobResponse {
  return {
    job_id: 'job-1',
    status: 'pending',
    created_at: '2026-02-18T00:00:00Z',
    mode: 'standard',
    markets: 'prime',
    recentDays: 10,
    sortBy: 'matchedDate',
    order: 'desc',
    ...overrides,
  };
}

vi.mock('@/stores/chartStore', () => ({
  useChartStore: () => mockChartStore,
}));

vi.mock('@tanstack/react-router', () => ({
  useNavigate: () => mockNavigate,
}));

vi.mock('@/hooks/useScreening', () => ({
  useRunScreeningJob: () => ({
    mutateAsync: (...args: unknown[]) => mockRunScreeningJob(...args),
    isPending: false,
    data: null,
    error: null,
  }),
  useScreeningJobStatus: (...args: unknown[]) => mockUseScreeningJobStatus(...args),
  useScreeningResult: (...args: unknown[]) => mockUseScreeningResult(...args),
  useCancelScreeningJob: () => ({
    mutate: (...args: unknown[]) => mockCancelScreeningJob(...args),
    isPending: false,
  }),
}));

vi.mock('@/hooks/useRanking', () => ({
  useRanking: () => ({
    data: { rankings: [] },
    isLoading: false,
    error: null,
  }),
}));

vi.mock('@/hooks/useFundamentalRanking', () => ({
  useFundamentalRanking: () => ({
    data: { metricKey: 'eps_forecast_to_actual', rankings: { ratioHigh: [], ratioLow: [] } },
    isLoading: false,
    error: null,
  }),
}));

vi.mock('@/hooks/useBacktest', () => ({
  useStrategies: () => mockStrategiesQueryResult,
}));

vi.mock('@/components/Screening/ScreeningFilters', () => ({
  ScreeningFilters: (props: unknown) => mockScreeningFilters(props),
}));

vi.mock('@/components/Screening/ScreeningJobProgress', () => ({
  ScreeningJobProgress: () => <div>Screening Job Progress</div>,
  ScreeningJobStatusInline: ({ job }: { job: ScreeningJobResponse }) => <div>Screening Job: {job.status}</div>,
}));

vi.mock('@/components/Screening/ScreeningSummary', () => ({
  ScreeningSummary: () => <div>Screening Summary</div>,
}));

vi.mock('@/components/Screening/ScreeningTable', () => ({
  ScreeningTable: (props: { onStockClick: (code: string) => void }) => mockScreeningTable(props),
}));

vi.mock('@/components/Ranking', () => ({
  RankingFilters: () => <div>Ranking Filters</div>,
  RankingSummary: () => <div>Ranking Summary</div>,
  RankingTable: ({ onStockClick }: { onStockClick: (code: string) => void }) => (
    <button type="button" onClick={() => onStockClick('6758')}>
      Ranking Row
    </button>
  ),
}));

vi.mock('@/components/FundamentalRanking', () => ({
  FundamentalRankingFilters: () => <div>Fundamental Ranking Filters</div>,
  FundamentalRankingSummary: () => <div>Fundamental Ranking Summary</div>,
  FundamentalRankingTable: ({ onStockClick }: { onStockClick: (code: string) => void }) => (
    <button type="button" onClick={() => onStockClick('9432')}>
      Fundamental Ranking Row
    </button>
  ),
}));

describe('AnalysisPage', () => {
  beforeEach(() => {
    mockStrategiesQueryResult = {
      data: {
        strategies: [
          { name: 'production/range_break_v15', category: 'production', screening_mode: 'standard' },
          { name: 'production/forward_eps_driven', category: 'production', screening_mode: 'standard' },
          {
            name: 'production/topix_gap_down_intraday_oracle',
            category: 'production',
            screening_mode: 'oracle',
          },
        ],
      },
      isLoading: false,
      error: null,
    };
    useAnalysisStore.persist?.clearStorage?.();
    useAnalysisStore.setState(createInitialAnalysisState());
    mockNavigate.mockReset();
    mockChartStore.setSelectedSymbol.mockReset();
    mockScreeningFilters.mockClear();
    mockScreeningTable.mockClear();
    mockRunScreeningJob.mockResolvedValue({
      job_id: 'job-1',
      status: 'pending',
    });
    mockUseScreeningJobStatus.mockReturnValue({
      data: null,
      error: null,
    });
    mockUseScreeningResult.mockReturnValue({
      data: null,
      error: null,
    });
    mockCancelScreeningJob.mockReset();
  });

  it('passes only standard production strategy names to screening filters', () => {
    render(<AnalysisPage />);

    expect(mockScreeningFilters).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: 'standard',
        strategyOptions: ['production/forward_eps_driven', 'production/range_break_v15'],
      })
    );
  });

  it('shows oracle-only strategies in oracle screening', async () => {
    const user = userEvent.setup();
    render(<AnalysisPage />);

    await user.click(screen.getByRole('button', { name: 'Oracle Screening' }));

    expect(mockScreeningFilters).toHaveBeenLastCalledWith(
      expect.objectContaining({
        mode: 'oracle',
        strategyOptions: ['production/topix_gap_down_intraday_oracle'],
      })
    );
  });

  it('preserves saved strategy selections until the strategy catalog is loaded, then sanitizes invalid names', async () => {
    useAnalysisStore.setState({
      screeningParams: {
        ...useAnalysisStore.getState().screeningParams,
        strategies: 'production/range_break_v15,production/missing_standard',
      },
      oracleScreeningParams: {
        ...useAnalysisStore.getState().oracleScreeningParams,
        strategies: 'production/topix_gap_down_intraday_oracle,production/missing_oracle',
      },
    });
    mockStrategiesQueryResult = {
      data: null,
      isLoading: true,
      error: null,
    };

    const { rerender } = render(<AnalysisPage />);

    expect(useAnalysisStore.getState().screeningParams.strategies).toBe(
      'production/range_break_v15,production/missing_standard'
    );
    expect(useAnalysisStore.getState().oracleScreeningParams.strategies).toBe(
      'production/topix_gap_down_intraday_oracle,production/missing_oracle'
    );

    mockStrategiesQueryResult = {
      data: {
        strategies: [
          { name: 'production/range_break_v15', category: 'production', screening_mode: 'standard' },
          {
            name: 'production/topix_gap_down_intraday_oracle',
            category: 'production',
            screening_mode: 'oracle',
          },
        ],
      },
      isLoading: false,
      error: null,
    };
    rerender(<AnalysisPage />);

    await waitFor(() => {
      expect(useAnalysisStore.getState().screeningParams.strategies).toBe('production/range_break_v15');
      expect(useAnalysisStore.getState().oracleScreeningParams.strategies).toBe(
        'production/topix_gap_down_intraday_oracle'
      );
    });
  });

  it('uses matchedDate descending as default standard screening sort when running', async () => {
    const user = userEvent.setup();
    render(<AnalysisPage />);

    await user.click(screen.getByRole('button', { name: 'Run Screening' }));

    expect(mockRunScreeningJob).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: 'standard',
        sortBy: 'matchedDate',
        order: 'desc',
      })
    );
  });

  it('runs oracle screening with oracle mode', async () => {
    const user = userEvent.setup();
    render(<AnalysisPage />);

    await user.click(screen.getByRole('button', { name: 'Oracle Screening' }));
    await user.click(screen.getByRole('button', { name: 'Run Oracle Screening' }));

    expect(mockRunScreeningJob).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: 'oracle',
      })
    );
  });

  it('renders screening view by default and switches to daily ranking', async () => {
    const user = userEvent.setup();
    render(<AnalysisPage />);

    expect(screen.getByText('Screening Filters')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Daily Ranking' }));
    expect(screen.getByText('Ranking Filters')).toBeInTheDocument();
  });

  it('switches to fundamental ranking tab', async () => {
    const user = userEvent.setup();
    render(<AnalysisPage />);

    await user.click(screen.getByRole('button', { name: 'Fundamental Ranking' }));
    expect(screen.getByText('Fundamental Ranking Filters')).toBeInTheDocument();
  });

  it('switches to oracle screening tab', async () => {
    const user = userEvent.setup();
    render(<AnalysisPage />);

    await user.click(screen.getByRole('button', { name: 'Oracle Screening' }));
    expect(screen.getByRole('button', { name: 'Run Oracle Screening' })).toBeInTheDocument();
  });

  it('navigates to chart when a stock is selected', async () => {
    const user = userEvent.setup();
    render(<AnalysisPage />);

    await user.click(screen.getByText('Screening Row'));
    expect(mockChartStore.setSelectedSymbol).toHaveBeenCalledWith('7203');
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/charts' });
  });

  it('restores cached screening result after remount', () => {
    useAnalysisStore.setState({
      screeningResult: createCachedScreeningResult(),
    });

    render(<AnalysisPage />);

    expect(mockScreeningTable).toHaveBeenCalledWith(
      expect.objectContaining({
        results: expect.arrayContaining([
          expect.objectContaining({
            stockCode: '7203',
          }),
        ]),
      })
    );
  });

  it('clears stale screening job id and keeps cached result visible on 404', async () => {
    useAnalysisStore.setState({
      activeScreeningJobId: 'stale-job',
      screeningResult: createCachedScreeningResult(),
    });
    mockUseScreeningJobStatus.mockReturnValue({
      data: null,
      error: new ApiError('ジョブが見つかりません: stale-job', 404),
    });

    render(<AnalysisPage />);

    await waitFor(() => {
      expect(useAnalysisStore.getState().activeScreeningJobId).toBeNull();
    });

    expect(mockScreeningTable).toHaveBeenCalledWith(
      expect.objectContaining({
        error: null,
        results: expect.arrayContaining([
          expect.objectContaining({
            stockCode: '7203',
          }),
        ]),
      })
    );
  });

  it('shows completed screening job status inline beside the run action', () => {
    mockUseScreeningJobStatus.mockReturnValue({
      data: createScreeningJob({ status: 'completed' }),
      error: null,
    });

    render(<AnalysisPage />);

    expect(screen.getByText('Screening Job: completed')).toBeInTheDocument();
    expect(screen.queryByText('Screening Job Progress')).not.toBeInTheDocument();
  });

  it('keeps screening history visibility separate for standard and oracle tabs', async () => {
    const user = userEvent.setup();
    useAnalysisStore.setState({
      screeningJobHistory: [createScreeningJob({ job_id: 'standard-job' })],
      oracleScreeningJobHistory: [createScreeningJob({ job_id: 'oracle-job', mode: 'oracle' })],
    });

    render(<AnalysisPage />);

    const standardToggle = screen.getByRole('switch', { name: 'Show History' });
    expect(standardToggle).toBeChecked();
    await user.click(standardToggle);
    expect(standardToggle).not.toBeChecked();

    await user.click(screen.getByRole('button', { name: 'Oracle Screening' }));

    const oracleToggle = screen.getByRole('switch', { name: 'Show History' });
    expect(oracleToggle).toBeChecked();
    expect(screen.getByText('oracle-j...')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Screening' }));

    const restoredStandardToggle = screen.getByRole('switch', { name: 'Show History' });
    expect(restoredStandardToggle).not.toBeChecked();
    expect(screen.queryByText('standard...')).not.toBeInTheDocument();
  });
});
