import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '@/lib/api-client';
import { createInitialAnalysisState, useAnalysisStore } from '@/stores/analysisStore';
import type { MarketScreeningResponse } from '@/types/screening';
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

function createCachedScreeningResult(): MarketScreeningResponse {
  return {
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
  useStrategies: () => ({
    data: {
      strategies: [
        { name: 'production/range_break_v15', category: 'production' },
        { name: 'production/forward_eps_driven', category: 'production' },
      ],
    },
    isLoading: false,
    error: null,
  }),
}));

vi.mock('@/components/Screening/ScreeningFilters', () => ({
  ScreeningFilters: (props: unknown) => mockScreeningFilters(props),
}));

vi.mock('@/components/Screening/ScreeningJobProgress', () => ({
  ScreeningJobProgress: () => <div>Screening Job Progress</div>,
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

  it('passes full production strategy names to screening filters', () => {
    render(<AnalysisPage />);

    expect(mockScreeningFilters).toHaveBeenCalledWith(
      expect.objectContaining({
        strategyOptions: ['production/forward_eps_driven', 'production/range_break_v15'],
      })
    );
  });

  it('uses matchedDate descending as default screening sort when running', async () => {
    const user = userEvent.setup();
    render(<AnalysisPage />);

    await user.click(screen.getByRole('button', { name: 'Run Screening' }));

    expect(mockRunScreeningJob).toHaveBeenCalledWith(
      expect.objectContaining({
        sortBy: 'matchedDate',
        order: 'desc',
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
});
