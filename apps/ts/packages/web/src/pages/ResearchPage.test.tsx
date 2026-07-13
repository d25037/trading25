import { render, screen, within } from '@testing-library/react';
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

function getCatalogItemRiskFlags(item: { experimentId: string; hasStructuredSummary: boolean }): string[] {
  if (!item.hasStructuredSummary) {
    return ['needs-publication-summary'];
  }
  return [];
}

const catalogItems = [
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
    experimentId: 'market-behavior/topix100-streak-3-53-transfer',
    runId: '20260406_133000_transfer01',
    title: 'TOPIX100 Streak 3/53 Transfer Study',
    objective: 'Retrospectively describe completed TOPIX100 events with fixed 3/53 state labels.',
    headline: 'Historical context only; not parameter-selection or tradeable evidence.',
    createdAt: '2026-04-06T13:30:00+00:00',
    analysisStartDate: '2016-01-01',
    analysisEndDate: '2026-03-31',
    gitCommit: '75c7a09b',
    tags: ['TOPIX100', 'streaks', 'multi-timeframe'],
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
  riskFlags: getCatalogItemRiskFlags(item),
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

function getFirstDataRow(): HTMLElement {
  const row = screen.getAllByRole('row').at(1);
  if (!row) {
    throw new Error('Expected at least one research data row');
  }
  return row;
}

describe('ResearchPage', () => {
  it('renders a table-first evidence matrix for research browsing', () => {
    render(<ResearchPage />);

    expect(screen.getAllByText('Evidence Matrix').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Research Workspace').length).toBeGreaterThan(0);
    expect(screen.getByRole('columnheader', { name: 'State' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Findings' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Readout & Risk' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Date' })).toBeInTheDocument();
    expect(screen.getByText('TOPIX Extreme Close-to-Close Mode')).toBeInTheDocument();
    expect(screen.getAllByText('Observed').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Candidate').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Market Regime').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Published Readout').length).toBeGreaterThan(0);
    expect(screen.getByText('Needs Readout')).toBeInTheDocument();
    expect(screen.queryByText('needs-publication-summary')).not.toBeInTheDocument();
    expect(screen.getAllByText('Keep as research evidence.').length).toBeGreaterThan(0);
  });

  it('filters the catalog by query and tag', async () => {
    const user = userEvent.setup();

    render(<ResearchPage />);

    const searchInput = screen.getByPlaceholderText('Search title, decision, experiment id, tag, or risk');

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

  it('sorts the evidence matrix by research date', async () => {
    const user = userEvent.setup();

    render(<ResearchPage />);

    expect(within(getFirstDataRow()).getByText('TOPIX100 Streak 3/53 Transfer Study')).toBeInTheDocument();

    await user.selectOptions(screen.getByLabelText('Sort'), 'oldest');

    expect(within(getFirstDataRow()).getByText('TOPIX Close Return Streaks')).toBeInTheDocument();
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
    await user.type(screen.getByPlaceholderText('Search title, decision, experiment id, tag, or risk'), 'no hit');
    expect(screen.getByText('No matching research')).toBeInTheDocument();
  });

  it('opens the selected analysis in the detail route', async () => {
    const user = userEvent.setup();

    render(<ResearchPage />);

    await user.click(screen.getByRole('row', { name: /Open TOPIX Close Return Streaks/ }));

    expect(mockNavigate).toHaveBeenCalledWith({
      to: '/research/detail',
      search: {
        experimentId: 'market-behavior/topix-close-return-streaks',
        runId: '20260405_120000_beta0001',
      },
    });
  });
});
