import type {
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
  resultSections: ResearchReadingSection[];
  considerationSections: ResearchReadingSection[];
  contextSections: ResearchReadingSection[];
  highlights: ResearchHighlight[];
  parameters: ResearchLabelValue[];
  tableHighlights: ResearchTableHighlight[];
}

interface ParsedMarkdownSection {
  title: string;
  items: string[];
}

const RESULT_SECTION_PATTERNS = [
  /current read/i,
  /readout/i,
  /forward/i,
  /validation/i,
  /selected/i,
  /best /i,
  /candidate/i,
  /discovery/i,
  /result/i,
  /takeaway/i,
];

const CONSIDERATION_SECTION_PATTERNS = [
  /consideration/i,
  /how to read/i,
  /interpret/i,
  /caveat/i,
  /implication/i,
  /use case/i,
  /reading note/i,
];

const CONTEXT_SECTION_PATTERNS = [/snapshot/i, /purpose/i, /method/i, /setup/i, /universe/i, /background/i];

const APPENDIX_SECTION_PATTERNS = [/artifact/i, /appendix/i, /stored table/i];

function cleanMarkdownText(value: string): string {
  return value
    .replace(/^[-*]\s+/, '')
    .replace(/^\d+\.\s+/, '')
    .replace(/`([^`]*)`/g, '$1')
    .replace(/\*\*([^*]+)\*\*/g, '$1')
    .replace(/\s+/g, ' ')
    .trim();
}

function isMarkdownTableLine(value: string): boolean {
  const trimmed = value.trim();
  if (!trimmed.startsWith('|')) {
    return false;
  }
  const withoutLeadingPipe = trimmed.slice(1);
  const withoutTrailingPipe = withoutLeadingPipe.endsWith('|') ? withoutLeadingPipe.slice(0, -1) : withoutLeadingPipe;
  return withoutTrailingPipe.split('|').length > 1;
}

function firstTextItem(sections: ResearchReadingSection[]): string | undefined {
  for (const section of sections) {
    const item = section.items.find((value) => !isMarkdownTableLine(value));
    if (item) {
      return item;
    }
  }
  return undefined;
}

function nonTableText(value?: string | null): string | undefined {
  if (!value) {
    return undefined;
  }
  return isMarkdownTableLine(cleanMarkdownText(value)) ? undefined : value;
}

function containsCleanedItem(sections: ResearchReadingSection[], value?: string | null): boolean {
  if (!value) {
    return false;
  }
  const cleanedValue = cleanMarkdownText(value);
  return sections.some((section) => section.items.some((item) => cleanMarkdownText(item) === cleanedValue));
}

function parseMarkdownSections(markdown: string): {
  title?: string;
  intro: string[];
  sections: ParsedMarkdownSection[];
} {
  const lines = markdown.split('\n');
  const intro: string[] = [];
  const sections: ParsedMarkdownSection[] = [];
  let title: string | undefined;
  let currentSection: ParsedMarkdownSection | null = null;

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) {
      continue;
    }

    if (line.startsWith('# ')) {
      const heading = cleanMarkdownText(line.slice(2));
      if (!title) {
        title = heading;
        currentSection = null;
        continue;
      }
      currentSection = { title: heading, items: [] };
      sections.push(currentSection);
      continue;
    }

    if (line.startsWith('## ')) {
      currentSection = { title: cleanMarkdownText(line.slice(3)), items: [] };
      sections.push(currentSection);
      continue;
    }

    const content = cleanMarkdownText(line);
    if (!content) {
      continue;
    }

    if (currentSection) {
      currentSection.items.push(content);
    } else {
      intro.push(content);
    }
  }

  return {
    title,
    intro,
    sections: sections.filter((section) => section.items.length > 0),
  };
}

function classifyMarkdownSection(
  section: ParsedMarkdownSection
): 'result' | 'consideration' | 'context' | 'appendix' {
  if (APPENDIX_SECTION_PATTERNS.some((pattern) => pattern.test(section.title))) {
    return 'appendix';
  }
  if (CONSIDERATION_SECTION_PATTERNS.some((pattern) => pattern.test(section.title))) {
    return 'consideration';
  }
  if (RESULT_SECTION_PATTERNS.some((pattern) => pattern.test(section.title))) {
    return 'result';
  }
  if (CONTEXT_SECTION_PATTERNS.some((pattern) => pattern.test(section.title))) {
    return 'context';
  }
  return 'context';
}

function buildStructuredReadingModel(detail: ResearchDetailResponse): ResearchReadingModel {
  const summary = detail.summary;
  if (!summary) {
    throw new Error('Structured reading model requires detail.summary.');
  }

  const resultSections =
    summary.resultBullets.length > 0
      ? [{ title: 'Key Takeaways', items: summary.resultBullets }]
      : [{ title: 'Current Read', items: [detail.item.headline ?? detail.item.objective ?? detail.item.title] }];

  const considerationSections =
    summary.considerations.length > 0
      ? [{ title: 'How To Read It', items: summary.considerations }]
      : [
          {
            title: 'Reading Note',
            items: ['This bundle did not publish explicit consideration notes. Read the result panel as the primary takeaway.'],
          },
        ];

  const contextSections: ResearchReadingSection[] = [];
  if (summary.purpose) {
    contextSections.push({ title: 'Purpose', items: [summary.purpose] });
  }
  if (summary.method.length > 0) {
    contextSections.push({ title: 'Method', items: summary.method });
  }

  return {
    headline: summary.resultHeadline ?? detail.item.headline ?? detail.item.objective ?? detail.item.title,
    resultSections,
    considerationSections,
    contextSections,
    highlights: summary.highlights,
    parameters: summary.selectedParameters,
    tableHighlights: summary.tableHighlights,
  };
}

function buildFallbackReadingModel(detail: ResearchDetailResponse): ResearchReadingModel {
  const parsed = parseMarkdownSections(detail.summaryMarkdown);
  const resultSections: ResearchReadingSection[] = [];
  const considerationSections: ResearchReadingSection[] = [];
  const contextSections: ResearchReadingSection[] = [];

  if (parsed.intro.length > 0) {
    contextSections.push({ title: 'Overview', items: parsed.intro });
  }

  for (const section of parsed.sections) {
    const normalizedSection: ResearchReadingSection = {
      title: section.title,
      items: section.items,
    };
    const sectionKind = classifyMarkdownSection(section);
    if (sectionKind === 'result') {
      resultSections.push(normalizedSection);
      continue;
    }
    if (sectionKind === 'consideration') {
      considerationSections.push(normalizedSection);
      continue;
    }
    if (sectionKind === 'context') {
      contextSections.push(normalizedSection);
    }
  }

  if (resultSections.length === 0) {
    resultSections.push({
      title: 'Current Read',
      items: [detail.item.headline ?? detail.item.objective ?? parsed.intro[0] ?? detail.item.title],
    });
  }

  if (considerationSections.length === 0) {
    considerationSections.push({
      title: 'Reading Note',
      items: [
        'This bundle was published without explicit consideration notes. Treat the promoted result sections as the main takeaway, and use the appendix below only as supporting detail.',
      ],
    });
  }

  const catalogHeadline =
    containsCleanedItem(contextSections, detail.item.headline) || isMarkdownTableLine(cleanMarkdownText(detail.item.headline ?? ''))
      ? undefined
      : detail.item.headline;
  const headline = catalogHeadline ?? firstTextItem(resultSections) ?? nonTableText(detail.item.objective) ?? parsed.title ?? detail.item.title;

  return {
    headline,
    resultSections,
    considerationSections,
    contextSections,
    highlights: [],
    parameters: [],
    tableHighlights: [],
  };
}

export function buildResearchReadingModel(detail: ResearchDetailResponse): ResearchReadingModel {
  return detail.summary ? buildStructuredReadingModel(detail) : buildFallbackReadingModel(detail);
}
