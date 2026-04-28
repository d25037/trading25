import type {
  PublishedReadoutSection,
  ResearchDetailResponse,
  ResearchHighlight,
  ResearchLabelValue,
  ResearchTableHighlight,
} from '@/types/research';

export interface ResearchReadingSection {
  title: string;
  items: string[];
}

export interface ResearchReadingModel {
  headline: string;
  hasPublishedReadout: boolean;
  missingSections: string[];
  decisionSections: ResearchReadingSection[];
  resultSections: ResearchReadingSection[];
  interpretationSections: ResearchReadingSection[];
  contextSections: ResearchReadingSection[];
  artifactSections: ResearchReadingSection[];
  highlights: ResearchHighlight[];
  parameters: ResearchLabelValue[];
  tableHighlights: ResearchTableHighlight[];
}

const REQUIRED_READOUT_SECTION_TITLES = [
  'Decision',
  'Main Findings',
  'Interpretation',
  'Production Implication',
  'Caveats',
  'Source Artifacts',
];

function normalizeTitle(value: string): string {
  return value.trim().toLowerCase().replace(/\s+/g, ' ');
}

function toReadingSection(section: PublishedReadoutSection): ResearchReadingSection | null {
  const title = section.title.trim();
  const items = section.items.map((item) => item.trim()).filter((item) => item.length > 0);
  if (!title || items.length === 0) {
    return null;
  }
  return { title, items };
}

function sectionMatches(section: ResearchReadingSection, titles: string[]): boolean {
  const normalized = normalizeTitle(section.title);
  return titles.some((title) => normalizeTitle(title) === normalized);
}

function filterSections(sections: ResearchReadingSection[], titles: string[]): ResearchReadingSection[] {
  return sections.filter((section) => sectionMatches(section, titles));
}

function findMissingReadoutSections(sections: ResearchReadingSection[]): string[] {
  return REQUIRED_READOUT_SECTION_TITLES.filter(
    (title) => !sections.some((section) => sectionMatches(section, [title]))
  );
}

function buildMissingReadoutModel(): ResearchReadingModel {
  return {
    headline:
      'Published Readout が未整備です。README または summary.json に publication-ready な readout を追加してください。',
    hasPublishedReadout: false,
    missingSections: REQUIRED_READOUT_SECTION_TITLES,
    decisionSections: [],
    resultSections: [],
    interpretationSections: [],
    contextSections: [],
    artifactSections: [],
    highlights: [],
    parameters: [],
    tableHighlights: [],
  };
}

export function buildResearchReadingModel(detail: ResearchDetailResponse): ResearchReadingModel {
  const summary = detail.summary;
  const readoutSections = (summary?.readoutSections ?? [])
    .map(toReadingSection)
    .filter((section): section is ResearchReadingSection => section !== null);

  if (readoutSections.length === 0) {
    return buildMissingReadoutModel();
  }

  const missingSections = findMissingReadoutSections(readoutSections);
  const decisionSection = filterSections(readoutSections, ['Decision'])[0];

  return {
    headline: summary?.decision ?? decisionSection?.items[0] ?? detail.item.decision ?? detail.item.title,
    hasPublishedReadout: missingSections.length === 0,
    missingSections,
    decisionSections: filterSections(readoutSections, ['Decision']),
    resultSections: filterSections(readoutSections, ['Main Findings']),
    interpretationSections: filterSections(readoutSections, ['Interpretation', 'Production Implication', 'Caveats']),
    contextSections: filterSections(readoutSections, ['Why This Research Was Run', 'Data Scope / PIT Assumptions']),
    artifactSections: filterSections(readoutSections, ['Source Artifacts']),
    highlights: summary?.highlights ?? [],
    parameters: summary?.selectedParameters ?? [],
    tableHighlights: summary?.tableHighlights ?? [],
  };
}
