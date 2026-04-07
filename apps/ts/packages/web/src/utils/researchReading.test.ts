import { describe, expect, it } from 'vitest';
import { buildResearchReadingModel } from './researchReading';
import type { ResearchDetailResponse } from '@/types/research';

function createDetail(overrides: Partial<ResearchDetailResponse>): ResearchDetailResponse {
  return {
    item: {
      experimentId: 'market-behavior/example',
      runId: '20260406_010000_example',
      title: 'Example Research',
      objective: 'Example objective',
      headline: 'Example headline',
      createdAt: '2026-04-06T00:00:00+00:00',
      analysisStartDate: '2020-01-01',
      analysisEndDate: '2026-03-31',
      gitCommit: '58c1fd4a',
      tags: ['example'],
      hasStructuredSummary: false,
    },
    summary: null,
    summaryMarkdown: '# Example Research',
    outputTables: ['summary_df'],
    availableRuns: [{ runId: '20260406_010000_example', createdAt: '2026-04-06T00:00:00+00:00', isLatest: true }],
    resultMetadata: {},
    ...overrides,
  };
}

describe('buildResearchReadingModel', () => {
  it('promotes markdown result sections above appendix sections', () => {
    const model = buildResearchReadingModel(
      createDetail({
        item: {
          experimentId: 'market-behavior/markdown',
          runId: '20260406_020000_markdown',
          title: 'Markdown Research',
          objective: 'Fallback objective',
          headline: 'Fallback headline',
          createdAt: '2026-04-06T00:00:00+00:00',
          analysisStartDate: '2020-01-01',
          analysisEndDate: '2026-03-31',
          gitCommit: '58c1fd4a',
          tags: ['fallback'],
          hasStructuredSummary: false,
        },
        summaryMarkdown: `# Markdown Research

Short intro paragraph.

## Snapshot

- Available range: \`2016-01-01 -> 2026-01-01\`

## Current Read

- Strongest grouped read remains weak and fades quickly.

## Validation Forward Snapshot

- 5d spread: +0.82%
- 20d spread: +1.55%

## Artifact Tables

- \`regime_market_df\`
- \`regime_summary_df\`
`,
      })
    );

    expect(model.headline).toBe('Fallback headline');
    expect(model.resultSections.map((section) => section.title)).toEqual(['Current Read', 'Validation Forward Snapshot']);
    expect(model.contextSections.map((section) => section.title)).toContain('Snapshot');
    expect(model.considerationSections[0]?.title).toBe('Reading Note');
  });

  it('uses structured summary fields as the reading model when available', () => {
    const model = buildResearchReadingModel(
      createDetail({
        item: {
          experimentId: 'market-behavior/structured',
          runId: '20260406_030000_structured',
          title: 'Structured Research',
          objective: 'Structured objective',
          headline: 'Structured headline',
          createdAt: '2026-04-06T00:00:00+00:00',
          analysisStartDate: '2020-01-01',
          analysisEndDate: '2026-03-31',
          gitCommit: '58c1fd4a',
          tags: ['structured'],
          hasStructuredSummary: true,
        },
        summary: {
          title: 'Structured Research',
          tags: ['structured'],
          purpose: 'Explain the analysis intent.',
          method: ['Evaluate discovery score.', 'Compare validation forward returns.'],
          resultHeadline: 'Streak mode separates segments but mean-reverts.',
          resultBullets: ['Selected X=3.', 'Bearish forward returns stayed stronger.'],
          considerations: ['Use as a regime label, not a direct trend-following signal.'],
          selectedParameters: [{ label: 'Selected X', value: '3 streaks' }],
          highlights: [{ label: 'Selected X', value: '3 streaks', tone: 'accent', detail: 'best score' }],
          tableHighlights: [{ name: 'mode_summary_df', label: 'Forward summary', description: '1/5/10/20d' }],
        },
      })
    );

    expect(model.headline).toBe('Streak mode separates segments but mean-reverts.');
    expect(model.resultSections[0]?.items).toContain('Selected X=3.');
    expect(model.considerationSections[0]?.items).toContain(
      'Use as a regime label, not a direct trend-following signal.'
    );
    expect(model.parameters[0]?.value).toBe('3 streaks');
  });
});
