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
    experimentId: 'market-behavior/topix100-strongest-setup-q10-threshold',
    runId: '20260406_160500_threshold01',
    title: 'TOPIX100 Strongest Setup vs Q10 Threshold',
    objective: 'Measure whether the strongest setup matters more than raw Q10 membership and how wide the lower-tail band can become.',
    headline: 'The strongest setup still beats non-strong Q10 alternatives even outside pure Q10.',
    createdAt: '2026-04-06T16:05:00+00:00',
    analysisStartDate: '2016-01-01',
    analysisEndDate: '2026-03-31',
    gitCommit: 'e678c0a5',
    tags: ['TOPIX100', 'threshold', 'streaks'],
    hasStructuredSummary: true,
  },
  {
    experimentId: 'market-behavior/topix100-short-side-streak-3-53-scan',
    runId: '20260406_171500_shortscan01',
    title: 'TOPIX100 Short Side Streak 3/53 Scan',
    objective: 'Scan the weakest short-side setup and the best strongest-vs-weakest pair trade under the transferred streak 3/53 state model.',
    headline: 'The weak side shifts away from the original Q2-Q4 hypothesis once streak state is added.',
    createdAt: '2026-04-06T17:15:00+00:00',
    analysisStartDate: '2016-01-01',
    analysisEndDate: '2026-03-31',
    gitCommit: '8314486a',
    tags: ['TOPIX100', 'short-side', 'pair-trade'],
    hasStructuredSummary: true,
  },
  {
    experimentId: 'market-behavior/topix100-streak-3-53-multivariate-priority',
    runId: '20260406_181500_priority01',
    title: 'TOPIX100 Streak 3/53 Multivariate Priority',
    objective: 'Quantify how much bucket, volume, short mode, and long mode each matter on the long and short side.',
    headline: 'Treat information itself as the variable and rank what is worth knowing first.',
    createdAt: '2026-04-06T18:15:00+00:00',
    analysisStartDate: '2016-01-01',
    analysisEndDate: '2026-03-31',
    gitCommit: 'c0eb7f87',
    tags: ['TOPIX100', 'multivariate', 'feature-priority'],
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
].map((item) => ({
  ...item,
  family: item.experimentId === 'market-behavior/unstructured-beta' ? 'Fallback' : 'Market Regime',
  status: item.experimentId === 'market-behavior/unstructured-beta' ? 'candidate' : 'observed',
  decision:
    item.experimentId === 'market-behavior/unstructured-beta'
      ? 'Needs a structured summary before promotion.'
      : 'Keep as research evidence.',
  promotedSurface: 'Research',
  riskFlags: item.hasStructuredSummary ? [] : ['markdown-only'],
  relatedExperiments: [],
}));

beforeEach(() => {
  vi.clearAllMocks();
  mockUseResearchCatalog.mockReturnValue({
    data: { items: catalogItems, lastUpdated: '2026-04-06T00:00:00+00:00' },
    isLoading: false,
    error: null,
  });
});

describe('ResearchPage', () => {
  it('renders a table-first evidence matrix for research browsing', () => {
    render(<ResearchPage />);

    expect(screen.getAllByText('Evidence Matrix').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Research Workspace').length).toBeGreaterThan(0);
    expect(screen.getByRole('columnheader', { name: 'Status' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Family' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Decision' })).toBeInTheDocument();
    expect(screen.getByText('TOPIX Extreme Mode Mean-Reversion Comparison')).toBeInTheDocument();
    expect(screen.getByText('TOPIX Extreme Close-to-Close Mode')).toBeInTheDocument();
    expect(screen.getAllByText('Observed').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Candidate').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Market Regime').length).toBeGreaterThan(0);
  });

  it('filters the catalog by query and tag', async () => {
    const user = userEvent.setup();

    render(<ResearchPage />);

    const searchInput = screen.getByPlaceholderText('Search title, finding, decision, experiment id, tag, or risk');

    await user.type(searchInput, 'close return');
    expect(screen.getByText('TOPIX Close Return Streaks')).toBeInTheDocument();
    expect(screen.queryByText('Beta Research')).not.toBeInTheDocument();

    await user.clear(searchInput);
    await user.selectOptions(screen.getByLabelText('Tag'), 'fallback');

    expect(screen.getByText('Beta Research')).toBeInTheDocument();
    expect(screen.queryByText('TOPIX Close Return Streaks')).not.toBeInTheDocument();
  });

  it('filters the evidence matrix by family and status', async () => {
    const user = userEvent.setup();

    render(<ResearchPage />);

    await user.selectOptions(screen.getByLabelText('Family'), 'Fallback');
    expect(screen.getByText('Beta Research')).toBeInTheDocument();
    expect(screen.queryByText('TOPIX Close Return Streaks')).not.toBeInTheDocument();

    await user.selectOptions(screen.getByLabelText('Family'), 'all');
    await user.selectOptions(screen.getByLabelText('Status'), 'candidate');
    expect(screen.getByText('Beta Research')).toBeInTheDocument();
    expect(screen.queryByText('TOPIX Close Return Streaks')).not.toBeInTheDocument();
  });

  it('renders loading, error, and empty workspace states', async () => {
    const user = userEvent.setup();

    mockUseResearchCatalog.mockReturnValueOnce({
      data: undefined,
      isLoading: true,
      error: null,
    });
    const { rerender } = render(<ResearchPage />);
    expect(screen.getByText('Loading research workspace...')).toBeInTheDocument();

    mockUseResearchCatalog.mockReturnValueOnce({
      data: undefined,
      isLoading: false,
      error: new Error('catalog unavailable'),
    });
    rerender(<ResearchPage />);
    expect(screen.getByText('Research load failed')).toBeInTheDocument();
    expect(screen.getByText('catalog unavailable')).toBeInTheDocument();

    mockUseResearchCatalog.mockReturnValueOnce({
      data: { items: catalogItems, lastUpdated: '2026-04-06T00:00:00+00:00' },
      isLoading: false,
      error: null,
    });
    rerender(<ResearchPage />);
    await user.type(screen.getByPlaceholderText('Search title, finding, decision, experiment id, tag, or risk'), 'no hit');
    expect(screen.getByText('No matching research')).toBeInTheDocument();
  });

  it('opens the selected analysis in the detail route', async () => {
    const user = userEvent.setup();

    render(<ResearchPage />);

    await user.click(screen.getByRole('button', { name: 'Open TOPIX Close Return Streaks' }));

    expect(mockNavigate).toHaveBeenCalledWith({
      to: '/research/detail',
      search: {
        experimentId: 'market-behavior/topix-close-return-streaks',
        runId: '20260405_120000_beta0001',
      },
    });
  });
});
