export type ResearchHighlightTone = 'neutral' | 'accent' | 'success' | 'warning' | 'danger';

export interface ResearchLabelValue {
  label: string;
  value: string;
}

export interface ResearchHighlight {
  label: string;
  value: string;
  tone: ResearchHighlightTone;
  detail?: string | null;
}

export interface ResearchTableHighlight {
  name: string;
  label: string;
  description?: string | null;
}

export interface PublishedResearchSummary {
  title: string;
  tags: string[];
  purpose: string;
  method: string[];
  resultHeadline?: string | null;
  resultBullets: string[];
  considerations: string[];
  selectedParameters: ResearchLabelValue[];
  highlights: ResearchHighlight[];
  tableHighlights: ResearchTableHighlight[];
}

export interface ResearchCatalogItem {
  experimentId: string;
  runId: string;
  title: string;
  objective?: string | null;
  headline?: string | null;
  createdAt: string;
  analysisStartDate?: string | null;
  analysisEndDate?: string | null;
  gitCommit?: string | null;
  tags: string[];
  hasStructuredSummary: boolean;
}

export interface ResearchRunReference {
  runId: string;
  createdAt: string;
  isLatest: boolean;
}

export interface ResearchCatalogResponse {
  items: ResearchCatalogItem[];
  lastUpdated: string;
}

export interface ResearchDetailResponse {
  item: ResearchCatalogItem;
  summary?: PublishedResearchSummary | null;
  summaryMarkdown: string;
  outputTables: string[];
  availableRuns: ResearchRunReference[];
  resultMetadata: Record<string, unknown>;
}
