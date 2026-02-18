import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { AnalysisPage } from './AnalysisPage';

const mockNavigate = vi.fn();

const mockChartStore = {
  setSelectedSymbol: vi.fn(),
};

const mockScreeningFilters = vi.fn((_props: unknown) => <div>Screening Filters</div>);
const mockRunScreeningJob = vi.fn().mockResolvedValue({
  job_id: 'job-1',
  status: 'pending',
});

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
  useScreeningJobStatus: () => ({
    data: null,
    error: null,
  }),
  useScreeningResult: () => ({
    data: null,
    error: null,
  }),
  useCancelScreeningJob: () => ({
    mutate: vi.fn(),
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
  ScreeningTable: ({ onStockClick }: { onStockClick: (code: string) => void }) => (
    <button type="button" onClick={() => onStockClick('7203')}>
      Screening Row
    </button>
  ),
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

describe('AnalysisPage', () => {
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

  it('renders screening view by default and switches to ranking', async () => {
    const user = userEvent.setup();
    render(<AnalysisPage />);

    expect(screen.getByText('Screening Filters')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: /Ranking/i }));
    expect(screen.getByText('Ranking Filters')).toBeInTheDocument();
  });

  it('navigates to chart when a stock is selected', async () => {
    const user = userEvent.setup();
    render(<AnalysisPage />);

    await user.click(screen.getByText('Screening Row'));
    expect(mockChartStore.setSelectedSymbol).toHaveBeenCalledWith('7203');
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/charts' });
  });
});
