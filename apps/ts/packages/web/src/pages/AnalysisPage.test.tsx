import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { AnalysisPage } from './AnalysisPage';

const mockChartStore = {
  setSelectedSymbol: vi.fn(),
};

const mockUiStore = {
  setActiveTab: vi.fn(),
};

vi.mock('@/stores/chartStore', () => ({
  useChartStore: () => mockChartStore,
}));

vi.mock('@/stores/uiStore', () => ({
  useUiStore: () => mockUiStore,
}));

vi.mock('@/hooks/useScreening', () => ({
  useScreening: () => ({
    data: { summary: {}, markets: [], recentDays: 0, referenceDate: '2024-01-01', results: [] },
    isLoading: false,
    error: null,
  }),
}));

vi.mock('@/hooks/useRanking', () => ({
  useRanking: () => ({
    data: { rankings: [] },
    isLoading: false,
    error: null,
  }),
}));

vi.mock('@/components/Screening/ScreeningFilters', () => ({
  ScreeningFilters: () => <div>Screening Filters</div>,
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
    expect(mockUiStore.setActiveTab).toHaveBeenCalledWith('charts');
  });
});
