import { describe, expect, it } from 'vitest';
import type { ResearchDetailResponse } from '@/types/research';
import { buildResearchReadingModel } from './researchReading';

const baseResearchMetadata = {
  family: 'Market Regime',
  status: 'observed' as const,
  decision: null,
  promotedSurface: 'Research',
  riskFlags: [],
  relatedExperiments: [],
};

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
      ...baseResearchMetadata,
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
          ...baseResearchMetadata,
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
    expect(model.resultSections.map((section) => section.title)).toEqual([
      'Current Read',
      'Validation Forward Snapshot',
    ]);
    expect(model.contextSections.map((section) => section.title)).toContain('Snapshot');
    expect(model.considerationSections[0]?.title).toBe('Reading Note');
  });

  it('does not use context bullets as the fallback headline for table-heavy markdown', () => {
    const model = buildResearchReadingModel(
      createDetail({
        item: {
          experimentId: 'market-behavior/accumulation-flow-followthrough',
          runId: '20260421_141732_82dc6d61',
          title: 'Accumulation Flow Followthrough',
          objective:
            '| Universe | Stocks | Analysis Stocks | Stock Days | Accum Events | Not Ext | Wick | | --- | --- | --- | --- | --- | --- | --- |',
          headline: 'Universes: `TOPIX500 / PRIME ex TOPIX500 / Standard / Growth`.',
          createdAt: '2026-04-21T14:17:32+00:00',
          analysisStartDate: '2016-04-21',
          analysisEndDate: '2026-04-20',
          gitCommit: '58c1fd4a',
          tags: [],
          hasStructuredSummary: false,
          ...baseResearchMetadata,
        },
        summaryMarkdown: `# Accumulation Flow Followthrough

## Scope

- Universes: \`TOPIX500 / PRIME ex TOPIX500 / Standard / Growth\`.
- Entry assumption: next session open after the signal date.

## Best Event Buckets (20d)

| Universe | Filter | Events | Mean |
| --- | --- | --- | --- |
| TOPIX500 | Accumulation + not extended | 224537 | 0.010545 |
`,
      })
    );

    expect(model.headline).toBe('Accumulation Flow Followthrough');
    expect(model.resultSections[0]?.title).toBe('Best Event Buckets (20d)');
    expect(model.resultSections[0]?.items[0]).toBe('| Universe | Filter | Events | Mean |');
    expect(model.contextSections[0]?.items[0]).toBe('Universes: TOPIX500 / PRIME ex TOPIX500 / Standard / Growth.');
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
          ...baseResearchMetadata,
        },
        summary: {
          title: 'Structured Research',
          tags: ['structured'],
          family: 'Market Regime',
          status: 'observed',
          decision: null,
          promotedSurface: 'Research',
          riskFlags: [],
          relatedExperiments: [],
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

  it('promotes docs-only current findings over implementation surface bullets', () => {
    const model = buildResearchReadingModel(
      createDetail({
        item: {
          experimentId: 'market-behavior/annual-first-open-last-close-fundamental-panel',
          runId: 'docs',
          title: 'Annual First-Open Last-Close Fundamental Panel',
          objective: 'Annual holding return study.',
          headline: 'Domain:',
          createdAt: '2026-04-23T00:00:00+00:00',
          analysisStartDate: null,
          analysisEndDate: null,
          gitCommit: null,
          tags: [],
          hasStructuredSummary: false,
          ...baseResearchMetadata,
        },
        summaryMarkdown: `# Annual First-Open Last-Close Fundamental Panel

Annual holding return study.

## Current Surface

- Domain:
  - \`apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py\`
- Runner:
  - \`apps/bt/scripts/research/run_annual_first_open_last_close_fundamental_panel.py\`

## Current Findings

- Baseline result: \`baseline-2026-04-23.md\`
- Low PBR + small cap was the strongest cross condition.

## Caveats

- Factor buckets are observational.
`,
      })
    );

    expect(model.headline).toBe('Low PBR + small cap was the strongest cross condition.');
    expect(model.resultSections.map((section) => section.title)).toEqual(['Current Findings']);
    expect(model.resultSections[0]?.items).not.toContain('Baseline result: baseline-2026-04-23.md');
    expect(model.resultSections[0]?.items).toContain('Low PBR + small cap was the strongest cross condition.');
    expect(model.contextSections.map((section) => section.title)).not.toContain('Current Surface');
    expect(model.considerationSections[0]?.title).toBe('Caveats');
  });
});
