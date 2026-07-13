import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ResearchDetailPage } from './ResearchDetailPage';

let currentSearch: { experimentId?: string; runId?: string } = {};

const mockNavigate = vi.fn();
const mockUseResearchDetail = vi.fn();

vi.mock('@tanstack/react-router', () => ({
  useNavigate: () => mockNavigate,
}));

vi.mock('@/router', () => ({
  researchDetailRoute: {
    useSearch: () => currentSearch,
  },
}));

vi.mock('@/hooks/useResearch', () => ({
  useResearchDetail: (experimentId: string | null, runId?: string | null) => mockUseResearchDetail(experimentId, runId),
}));

beforeEach(() => {
  vi.clearAllMocks();
  currentSearch = {
    experimentId: 'market-behavior/topix-extreme-close-to-close-mode',
    runId: '20260406_091000_normal0001',
  };

  mockUseResearchDetail.mockImplementation((experimentId: string | null) => {
    if (experimentId === 'market-behavior/topix-extreme-close-to-close-mode') {
      return {
        data: {
          item: {
            experimentId: 'market-behavior/topix-extreme-close-to-close-mode',
            runId: '20260406_091000_normal0001',
            title: 'TOPIX Extreme Close-to-Close Mode',
            objective: 'Describe TOPIX mode from daily close-to-close shocks.',
            headline: 'Useful as a multi-timeframe market filter.',
            createdAt: '2026-04-06T09:10:00+00:00',
            analysisStartDate: '2016-01-01',
            analysisEndDate: '2026-03-31',
            gitCommit: '58c1fd4a',
            tags: ['TOPIX', 'streaks'],
            hasStructuredSummary: true,
          },
          summary: {
            title: 'TOPIX Extreme Close-to-Close Mode',
            tags: ['TOPIX', 'mode'],
            selectedParameters: [
              { label: 'Shock threshold', value: '2.0%' },
            ],
            highlights: [
              { label: 'Disposition', value: 'Observed', tone: 'neutral', detail: 'market-state evidence' },
            ],
            tableHighlights: [
              { name: 'mode_summary_df', label: 'Mode summary', description: 'descriptive outcomes' },
            ],
            readoutSections: [
              {
                title: 'Decision',
                items: ['Keep as a descriptive market-state filter.'],
              },
              {
                title: 'Main Findings',
                items: [
                  '#### Bearish shocks show the strongest rebound.',
                  '| Mode | Events | Mean |',
                  '| --- | ---: | ---: |',
                  '| Bearish | 128 | 1.29% |',
                  'The mode uses information available on the observation date.',
                ],
              },
              {
                title: 'Interpretation',
                items: ['Interpret as a market-state description, not a standalone ranking signal.'],
              },
              {
                title: 'Production Implication',
                items: ['Use only as a contextual filter.'],
              },
              {
                title: 'Caveats',
                items: ['Threshold sensitivity remains observational.'],
              },
              {
                title: 'Source Artifacts',
                items: ['`mode_summary_df`'],
              },
            ],
          },
          summaryMarkdown: `# TOPIX Extreme Close-to-Close Mode

Raw summary paragraph.

## Mode Summary

| Mode | Events | Mean |
| --- | ---: | ---: |
| Bearish | 128 | 1.29% |
`,
          outputTables: ['mode_summary_df'],
          availableRuns: [
            { runId: '20260406_091000_normal0001', createdAt: '2026-04-06T09:10:00+00:00', isLatest: true },
            { runId: '20260406_081000_normal0000', createdAt: '2026-04-06T08:10:00+00:00', isLatest: false },
          ],
          resultMetadata: {},
        },
        isLoading: false,
        error: null,
      };
    }

    if (experimentId === 'market-behavior/unstructured-beta') {
      return {
        data: {
          item: {
            experimentId: 'market-behavior/unstructured-beta',
            runId: '20260405_120000_beta0001',
            title: 'Beta Research',
            objective: 'Fallback objective',
            headline: 'Fallback headline',
            createdAt: '2026-04-05T12:00:00+00:00',
            analysisStartDate: '2023-01-01',
            analysisEndDate: '2023-12-31',
            gitCommit: '58c1fd4a',
            tags: [],
            hasStructuredSummary: false,
          },
          summary: null,
          summaryMarkdown: `# Beta Research

Beta fallback paragraph.

## Snapshot

- Available range: \`2023-01-01 -> 2023-12-31\`

## Current Read

- Beta fallback takeaway.

## Best Event Buckets (20d)

| Universe | Filter | Events | Mean |
| --- | --- | --- | --- |
| TOPIX500 | Accumulation pressure | 545411 | 0.009499 |

## Artifact Tables

- \`summary_df\`
`,
          outputTables: ['summary_df'],
          availableRuns: [
            { runId: '20260405_120000_beta0001', createdAt: '2026-04-05T12:00:00+00:00', isLatest: true },
          ],
          resultMetadata: {},
        },
        isLoading: false,
        error: null,
      };
    }

    if (experimentId === 'market-behavior/annual-first-open-last-close-fundamental-panel') {
      return {
        data: {
          item: {
            experimentId: 'market-behavior/annual-first-open-last-close-fundamental-panel',
            runId: 'docs',
            title: 'Annual First-Open Last-Close Fundamental Panel',
            objective: 'Annual holding return study with PIT-safe FY fundamentals.',
            headline: 'Low PBR + small cap was the strongest cross condition.',
            createdAt: '2026-04-23T08:38:01+00:00',
            analysisStartDate: null,
            analysisEndDate: null,
            gitCommit: null,
            tags: [],
            family: 'Annual Fundamentals',
            status: 'observed',
            decision: 'Keep low PBR + small cap as a ranking diagnostic until liquidity and cost checks are complete.',
            promotedSurface: 'Research',
            riskFlags: [],
            relatedExperiments: [],
            hasStructuredSummary: true,
          },
          summary: {
            title: 'Annual First-Open Last-Close Fundamental Panel',
            tags: [],
            family: 'Annual Fundamentals',
            status: 'observed',
            decision: 'Keep low PBR + small cap as a ranking diagnostic until liquidity and cost checks are complete.',
            promotedSurface: 'Research',
            riskFlags: [],
            relatedExperiments: [],
            selectedParameters: [],
            highlights: [],
            tableHighlights: [],
            readoutSections: [
              {
                title: 'Decision',
                items: [
                  'Keep low PBR + small cap as a ranking diagnostic until liquidity and cost checks are complete.',
                ],
              },
              {
                title: 'Main Findings',
                items: [
                  '#### Standard PBR Q1 + market-cap Q1 is the strongest branch.',
                  '| Condition | CAGR | Sharpe |',
                  '| --- | ---: | ---: |',
                  '| Standard PBR Q1 + market-cap Q1 | 37.7% | 2.16 |',
                ],
              },
              {
                title: 'Interpretation',
                items: ['The strongest branch is exposed to small-cap and low-ADV implementation risk.'],
              },
              {
                title: 'Production Implication',
                items: ['Use as ranking research input before production promotion.'],
              },
              {
                title: 'Caveats',
                items: ['Factor bucket is observational.'],
              },
              {
                title: 'Source Artifacts',
                items: ['baseline-2026-04-23.md'],
              },
            ],
          },
          summaryMarkdown: `# Annual First-Open Last-Close Fundamental Panel

Annual holding return study.

## Current Surface

- Domain:
  - \`apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py\`
- Runner:
  - \`apps/bt/scripts/research/run_annual_first_open_last_close_fundamental_panel.py\`

## Design

- Entry: first trading day open.
- Exit: last trading day close.

## Current Findings

- Baseline result: [baseline-2026-04-23.md](./baseline-2026-04-23.md)
- Low PBR + small cap was the strongest cross condition.

## Caveats

- Factor bucket is observational.
`,
          outputTables: [],
          availableRuns: [{ runId: 'docs', createdAt: '2026-04-23T08:38:01+00:00', isLatest: true }],
          resultMetadata: { source: 'docs' },
        },
        isLoading: false,
        error: null,
      };
    }

    return {
      data: undefined,
      isLoading: false,
      error: null,
    };
  });
});

describe('ResearchDetailPage', () => {
  it('renders the dedicated detail reading layout', () => {
    render(<ResearchDetailPage />);

    expect(screen.getByText('Back to catalog')).toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 2, name: 'Decision' })).toBeInTheDocument();
    expect(screen.getAllByText('Interpretation').length).toBeGreaterThan(0);
    expect(
      screen.getByRole('heading', { level: 4, name: 'Bearish shocks show the strongest rebound.' })
    ).toBeInTheDocument();
    expect(screen.getAllByRole('columnheader', { name: 'Events' }).length).toBeGreaterThan(0);
    expect(screen.getByText('The mode uses information available on the observation date.')).toBeInTheDocument();
    expect(screen.getByText('Raw Bundle Markdown')).toBeInTheDocument();
  });

  it('renders markdown pipe tables as structured tables', () => {
    render(<ResearchDetailPage />);

    const table = screen
      .getAllByRole('table', { name: 'Markdown table' })
      .find((candidate) => within(candidate).queryByRole('columnheader', { name: 'Mode' }));
    if (!table) {
      throw new Error('Expected raw markdown table to render.');
    }
    expect(within(table).getByRole('columnheader', { name: 'Mode' })).toBeInTheDocument();
    expect(within(table).getByRole('columnheader', { name: 'Events' })).toBeInTheDocument();
    expect(within(table).getByText('Bearish')).toBeInTheDocument();
    expect(within(table).getByText('1.29%')).toBeInTheDocument();
    expect(screen.queryByText('| Mode | Events | Mean |')).not.toBeInTheDocument();
  });

  it('requires Published Readout when no structured summary exists', () => {
    currentSearch = {
      experimentId: 'market-behavior/unstructured-beta',
      runId: '20260405_120000_beta0001',
    };

    render(<ResearchDetailPage />);

    expect(screen.getAllByText('Beta Research').length).toBeGreaterThan(0);
    expect(screen.getByText('Needs Published Readout')).toBeInTheDocument();
    expect(screen.getByText('Main Findings')).toBeInTheDocument();
  });

  it('renders docs-backed research as a dense findings-first page', () => {
    currentSearch = {
      experimentId: 'market-behavior/annual-first-open-last-close-fundamental-panel',
      runId: 'docs',
    };

    render(<ResearchDetailPage />);

    expect(screen.getByRole('heading', { level: 2, name: 'Decision' })).toBeInTheDocument();
    expect(screen.getByText('Standard PBR Q1 + market-cap Q1 is the strongest branch.')).toBeInTheDocument();
    expect(screen.queryByText('Needs Published Readout')).not.toBeInTheDocument();
    expect(screen.getByText('Scope And Source')).toBeInTheDocument();
    expect(screen.getByText('Source Markdown')).toBeInTheDocument();
  });

  it('keeps raw markdown tables in the appendix when readout is missing', () => {
    currentSearch = {
      experimentId: 'market-behavior/unstructured-beta',
      runId: '20260405_120000_beta0001',
    };

    render(<ResearchDetailPage />);

    const tables = screen.getAllByRole('table', { name: 'Markdown table' });
    expect(tables).toHaveLength(1);
    expect(within(tables[0] as HTMLElement).getByRole('columnheader', { name: 'Universe' })).toBeInTheDocument();
    expect(within(tables[0] as HTMLElement).getByText('Accumulation pressure')).toBeInTheDocument();
    expect(screen.queryByText('| Universe | Filter | Events | Mean |')).not.toBeInTheDocument();
  });

  it('navigates between runs and back to the catalog', async () => {
    const user = userEvent.setup();

    render(<ResearchDetailPage />);

    await user.click(screen.getByText('20260406_081000_normal0000'));
    expect(mockNavigate).toHaveBeenCalledWith({
      to: '/research/detail',
      search: {
        experimentId: 'market-behavior/topix-extreme-close-to-close-mode',
        runId: '20260406_081000_normal0000',
      },
    });

    await user.click(screen.getByText('Back to catalog'));
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/research' });
  });
});
