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
  it('does not promote raw markdown even when it contains a Published Readout section', () => {
    const model = buildResearchReadingModel(
      createDetail({
        summaryMarkdown: `# Markdown Research

## Published Readout

### Decision
- Keep this as the canonical readout.

### Main Findings
- Primary result was +12.3%.
`,
      })
    );

    expect(model.hasPublishedReadout).toBe(false);
    expect(model.resultSections).toEqual([]);
    expect(model.contextSections).toEqual([]);
    expect(model.missingSections).toContain('Main Findings');
  });

  it('uses Published Readout sections as the only promoted reading model', () => {
    const model = buildResearchReadingModel(
      createDetail({
        summary: {
          title: 'Structured Research',
          tags: ['structured'],
          family: 'Market Regime',
          status: 'observed',
          decision: 'Treat as a regime label.',
          promotedSurface: 'Research',
          riskFlags: [],
          relatedExperiments: [],
          readoutSections: [
            { title: 'Decision', items: ['Treat as a regime label.'] },
            { title: 'Why This Research Was Run', items: ['Find a reusable regime readout.'] },
            { title: 'Data Scope / PIT Assumptions', items: ['PIT-safe daily data.'] },
            {
              title: 'Main Findings',
              items: ['#### Selected X=3.', '| Scope | Value |', '| --- | ---: |', '| X | `3` |'],
            },
            { title: 'Interpretation', items: ['Use as a label, not a direct signal.'] },
            { title: 'Production Implication', items: ['Keep it in ranking research.'] },
            { title: 'Caveats', items: ['Validation is historical.'] },
            { title: 'Source Artifacts', items: ['`results.duckdb`'] },
          ],
          selectedParameters: [{ label: 'Selected X', value: '3 streaks' }],
          highlights: [{ label: 'Selected X', value: '3 streaks', tone: 'accent', detail: 'best score' }],
          tableHighlights: [{ name: 'mode_summary_df', label: 'Forward summary', description: '1/5/10/20d' }],
        },
      })
    );

    expect(model.hasPublishedReadout).toBe(true);
    expect(model.headline).toBe('Treat as a regime label.');
    expect(model.decisionSections[0]?.items).toEqual(['Treat as a regime label.']);
    expect(model.resultSections[0]?.items).toContain('#### Selected X=3.');
    expect(model.interpretationSections.map((section) => section.title)).toEqual([
      'Interpretation',
      'Production Implication',
      'Caveats',
    ]);
    expect(model.contextSections.map((section) => section.title)).toEqual([
      'Why This Research Was Run',
      'Data Scope / PIT Assumptions',
    ]);
    expect(model.artifactSections[0]?.title).toBe('Source Artifacts');
    expect(model.parameters[0]?.value).toBe('3 streaks');
  });

  it('marks incomplete Published Readout summaries as not published', () => {
    const model = buildResearchReadingModel(
      createDetail({
        summary: {
          title: 'Incomplete Research',
          tags: [],
          family: 'Market Regime',
          status: 'observed',
          decision: 'Incomplete.',
          promotedSurface: 'Research',
          riskFlags: [],
          relatedExperiments: [],
          readoutSections: [
            { title: 'Decision', items: ['Incomplete.'] },
            { title: 'Main Findings', items: ['One result.'] },
          ],
          selectedParameters: [],
          highlights: [],
          tableHighlights: [],
        },
      })
    );

    expect(model.hasPublishedReadout).toBe(false);
    expect(model.missingSections).toEqual(['Interpretation', 'Production Implication', 'Caveats', 'Source Artifacts']);
  });
});
