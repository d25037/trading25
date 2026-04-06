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
    experimentId: 'market-behavior/topix-extreme-mode-mean-reversion-comparison',
    runId: '20260406_090000_compare0001',
    title: 'TOPIX Extreme Mode Mean-Reversion Comparison',
    objective: 'Compare the normal and streak mode definitions under the same execution assumptions.',
    headline: 'Use streak mode for execution and normal mode for context.',
    createdAt: '2026-04-06T09:00:00+00:00',
    analysisStartDate: '2016-01-01',
    analysisEndDate: '2026-03-31',
    gitCommit: '8dc36bd0',
    tags: ['TOPIX', 'comparison', 'mode'],
    hasStructuredSummary: true,
  },
  {
    experimentId: 'market-behavior/topix-extreme-close-to-close-mode',
    runId: '20260406_091000_normal0001',
    title: 'TOPIX Extreme Close-to-Close Mode',
    objective: 'Use the largest daily close-to-close shock to classify TOPIX mode.',
    headline: 'Better as a multi-timeframe filter than as a standalone signal.',
    createdAt: '2026-04-06T09:10:00+00:00',
    analysisStartDate: '2016-01-01',
    analysisEndDate: '2026-03-31',
    gitCommit: '8dc36bd0',
    tags: ['TOPIX', 'mode', 'daily'],
    hasStructuredSummary: true,
  },
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
    experimentId: 'market-behavior/topix-streak-multi-timeframe-mode',
    runId: '20260406_092500_pairscan01',
    title: 'TOPIX Streak Multi-Timeframe Mode',
    objective: 'Scan short and long streak windows to build a four-state TOPIX regime.',
    headline: 'A short streak trigger plus a slower streak filter creates the cleanest 4-state split.',
    createdAt: '2026-04-06T09:25:00+00:00',
    analysisStartDate: '2016-01-01',
    analysisEndDate: '2026-03-31',
    gitCommit: '8dc36bd0',
    tags: ['TOPIX', 'streaks', 'multi-timeframe'],
    hasStructuredSummary: true,
  },
  {
    experimentId: 'market-behavior/topix100-streak-3-53-transfer',
    runId: '20260406_133000_transfer01',
    title: 'TOPIX100 Streak 3/53 Transfer Study',
    objective: 'Apply the fixed TOPIX 3/53 streak pair to TOPIX100 constituents.',
    headline: 'A stock-level transfer test for the TOPIX-learned streak hierarchy.',
    createdAt: '2026-04-06T13:30:00+00:00',
    analysisStartDate: '2016-01-01',
    analysisEndDate: '2026-03-31',
    gitCommit: '75c7a09b',
    tags: ['TOPIX100', 'streaks', 'multi-timeframe'],
    hasStructuredSummary: true,
  },
  {
    experimentId: 'market-behavior/topix100-q10-bounce-streak-3-53-conditioning',
    runId: '20260406_143500_fusion0001',
    title: 'TOPIX100 Q10 Bounce x Streak 3/53 Conditioning',
    objective: 'Fuse the TOPIX100 bounce bucket lens with the fixed streak 3/53 state model.',
    headline: 'Use streak state as the execution filter for the Q10 bounce bucket.',
    createdAt: '2026-04-06T14:35:00+00:00',
    analysisStartDate: '2016-01-01',
    analysisEndDate: '2026-03-31',
    gitCommit: '75c7a09b',
    tags: ['TOPIX100', 'bucket', 'streaks'],
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
  it('renders curated TOPIX mode studies alongside the discovery-focused catalog', () => {
    render(<ResearchPage />);

    expect(screen.getByText('Playground Analyses')).toBeInTheDocument();
    expect(screen.getByText('TOPIX Mode Studies')).toBeInTheDocument();
    expect(screen.getByText('TOPIX Extreme Mode Mean-Reversion Comparison')).toBeInTheDocument();
    expect(screen.getByText('TOPIX Extreme Close-to-Close Mode')).toBeInTheDocument();
    expect(screen.getByText('TOPIX Streak Multi-Timeframe Mode')).toBeInTheDocument();
    expect(screen.getByText('TOPIX100 Streak 3/53 Transfer Study')).toBeInTheDocument();
    expect(screen.getByText('TOPIX100 Q10 Bounce x Streak 3/53 Conditioning')).toBeInTheDocument();
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
