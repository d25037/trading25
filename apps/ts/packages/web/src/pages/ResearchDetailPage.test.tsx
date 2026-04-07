import { render, screen } from '@testing-library/react';
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
    experimentId: 'market-behavior/topix-streak-extreme-mode',
    runId: '20260405_110000_alpha0002',
  };

  mockUseResearchDetail.mockImplementation((experimentId: string | null) => {
    if (experimentId === 'market-behavior/topix-streak-extreme-mode') {
      return {
        data: {
          item: {
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
          summary: {
            title: 'TOPIX Streak Extreme Mode',
            tags: ['TOPIX', 'streaks'],
            purpose: 'Use the dominant streak candle to classify TOPIX mode.',
            method: ['Collapse returns into streak candles.', 'Pick the dominant streak return.'],
            resultHeadline: 'Mode labels separate segments but still mean-revert.',
            resultBullets: ['Selected X=3 streaks.', 'Bearish forward returns were stronger than bullish.'],
            considerations: ['Interpret as regime labeling, not direct trend following.'],
            selectedParameters: [{ label: 'Selected X', value: '3 streaks' }],
            highlights: [{ label: 'Selected X', value: '3 streaks', tone: 'accent', detail: 'best discovery score' }],
            tableHighlights: [{ name: 'mode_summary_df', label: 'Forward return summary', description: '1/5/10/20d' }],
          },
          summaryMarkdown: '# TOPIX Streak Extreme Mode\n\nRaw summary paragraph.\n',
          outputTables: ['mode_summary_df', 'window_score_df'],
          availableRuns: [
            { runId: '20260405_110000_alpha0002', createdAt: '2026-04-05T11:00:00+00:00', isLatest: true },
            { runId: '20260405_100000_alpha0001', createdAt: '2026-04-05T10:00:00+00:00', isLatest: false },
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

## Artifact Tables

- \`summary_df\`
`,
          outputTables: ['summary_df'],
          availableRuns: [{ runId: '20260405_120000_beta0001', createdAt: '2026-04-05T12:00:00+00:00', isLatest: true }],
          resultMetadata: {},
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
    expect(screen.getByText('What It Found')).toBeInTheDocument();
    expect(screen.getAllByText('How To Read It').length).toBeGreaterThan(0);
    expect(screen.getByText('Selected X=3 streaks.')).toBeInTheDocument();
    expect(screen.getByText('Raw Bundle Markdown')).toBeInTheDocument();
  });

  it('falls back to markdown when no structured summary exists', () => {
    currentSearch = {
      experimentId: 'market-behavior/unstructured-beta',
      runId: '20260405_120000_beta0001',
    };

    render(<ResearchDetailPage />);

    expect(screen.getAllByText('Beta Research').length).toBeGreaterThan(0);
    expect(screen.getByText('What It Found')).toBeInTheDocument();
    expect(screen.getAllByText('Current Read').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Beta fallback takeaway.').length).toBeGreaterThan(0);
    expect(screen.getAllByText('How To Read It').length).toBeGreaterThan(0);
  });

  it('navigates between runs and back to the catalog', async () => {
    const user = userEvent.setup();

    render(<ResearchDetailPage />);

    await user.click(screen.getByText('20260405_100000_alpha0001'));
    expect(mockNavigate).toHaveBeenCalledWith({
      to: '/research/detail',
      search: {
        experimentId: 'market-behavior/topix-streak-extreme-mode',
        runId: '20260405_100000_alpha0001',
      },
    });

    await user.click(screen.getByText('Back to catalog'));
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/research' });
  });
});
