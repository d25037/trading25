import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ResearchPage } from './ResearchPage';

const mockNavigate = vi.fn();
const mockUseResearchCatalog = vi.fn();

vi.mock('@tanstack/react-router', () => ({
  useNavigate: () => mockNavigate,
}));

vi.mock('@/hooks/useResearch', () => ({
  useResearchCatalog: () => mockUseResearchCatalog(),
}));

const catalogItems = [
  {
    experimentId: 'market-behavior/topix-streak-extreme-mode',
    runId: '20260405_110000_alpha0002',
    title: 'TOPIX Streak Extreme Mode',
    objective: 'Use the dominant streak candle to classify TOPIX mode.',
    headline: 'Short-memory mode with mean-reversion bias.',
    createdAt: '2026-04-05T11:00:00+00:00',
    analysisStartDate: '2016-01-01',
    analysisEndDate: '2026-03-31',
    gitCommit: '58c1fd4a',
    tags: ['TOPIX', 'streaks'],
    hasStructuredSummary: true,
  },
  {
    experimentId: 'market-behavior/topix-close-return-streaks',
    runId: '20260405_120000_beta0001',
    title: 'TOPIX Close Return Streaks',
    objective: 'Composite streak candles and their post-completion behavior.',
    headline: 'Completed bearish streaks bounce quickly.',
    createdAt: '2026-04-05T12:00:00+00:00',
    analysisStartDate: '2016-01-01',
    analysisEndDate: '2026-03-31',
    gitCommit: '58c1fd4a',
    tags: ['TOPIX', 'mean-reversion'],
    hasStructuredSummary: true,
  },
  {
    experimentId: 'market-behavior/unstructured-beta',
    runId: '20260405_130000_gamma0001',
    title: 'Beta Research',
    objective: 'Fallback objective',
    headline: 'Fallback headline',
    createdAt: '2026-04-05T13:00:00+00:00',
    analysisStartDate: '2023-01-01',
    analysisEndDate: '2023-12-31',
    gitCommit: '58c1fd4a',
    tags: ['fallback'],
    hasStructuredSummary: false,
  },
];

beforeEach(() => {
  vi.clearAllMocks();
  mockUseResearchCatalog.mockReturnValue({
    data: { items: catalogItems, lastUpdated: '2026-04-06T00:00:00+00:00' },
    isLoading: false,
    error: null,
  });
});

describe('ResearchPage', () => {
  it('renders a discovery-focused catalog with featured and library sections', () => {
    render(<ResearchPage />);

    expect(screen.getByText('Playground Analyses')).toBeInTheDocument();
    expect(screen.getByText('Start With The Latest High-Signal Bundle')).toBeInTheDocument();
    expect(screen.getByText('More Published Analyses')).toBeInTheDocument();
    expect(screen.getAllByText('TOPIX Streak Extreme Mode').length).toBeGreaterThan(0);
    expect(screen.getByText('TOPIX Close Return Streaks')).toBeInTheDocument();
  });

  it('filters the catalog by query and tag', async () => {
    const user = userEvent.setup();

    render(<ResearchPage />);

    await user.type(screen.getByPlaceholderText('Search title, takeaway, experiment id, or tag'), 'close return');
    expect(screen.getByText('TOPIX Close Return Streaks')).toBeInTheDocument();
    expect(screen.queryByText('Beta Research')).not.toBeInTheDocument();

    await user.clear(screen.getByPlaceholderText('Search title, takeaway, experiment id, or tag'));
    await user.click(screen.getByRole('button', { name: 'fallback' }));

    expect(screen.getByText('Beta Research')).toBeInTheDocument();
    expect(screen.queryByText('TOPIX Close Return Streaks')).not.toBeInTheDocument();
  });

  it('opens the selected analysis in the detail route', async () => {
    const user = userEvent.setup();

    render(<ResearchPage />);

    await user.click(screen.getByText('TOPIX Close Return Streaks'));

    expect(mockNavigate).toHaveBeenCalledWith({
      to: '/research/detail',
      search: {
        experimentId: 'market-behavior/topix-close-return-streaks',
        runId: '20260405_120000_beta0001',
      },
    });
  });
});
