import type { components } from '@trading25/contracts/clients/backtest/generated/bt-api-types';

export type ResearchHighlightTone = components['schemas']['ResearchHighlight']['tone'];
export type ResearchDecisionStatus = components['schemas']['ResearchCatalogItem']['status'];

export type ApiResearchLabelValue = components['schemas']['ResearchLabelValue'];
export type ApiResearchHighlight = components['schemas']['ResearchHighlight'];
export type ApiResearchTableHighlight = components['schemas']['ResearchTableHighlight'];
export type ApiPublishedResearchSummary = components['schemas']['PublishedResearchSummary'];
export type ApiResearchCatalogItem = components['schemas']['ResearchCatalogItem'];
export type ApiResearchRunReference = components['schemas']['ResearchRunReference'];
export type ApiResearchCatalogResponse = components['schemas']['ResearchCatalogResponse'];
export type ApiResearchDetailResponse = components['schemas']['ResearchDetailResponse'];

export type ResearchLabelValue = ApiResearchLabelValue;

export interface ResearchHighlight extends Omit<ApiResearchHighlight, 'tone'> {
  tone: ResearchHighlightTone;
}

export type ResearchTableHighlight = ApiResearchTableHighlight;

export interface PublishedReadoutSection {
  title: string;
  items: string[];
}

export interface PublishedResearchSummary
  extends Omit<
    ApiPublishedResearchSummary,
    | 'tags'
    | 'selectedParameters'
    | 'highlights'
    | 'tableHighlights'
    | 'riskFlags'
    | 'relatedExperiments'
    | 'readoutSections'
  > {
  tags: string[];
  selectedParameters: ResearchLabelValue[];
  highlights: ResearchHighlight[];
  tableHighlights: ResearchTableHighlight[];
  riskFlags: string[];
  relatedExperiments: string[];
  readoutSections: PublishedReadoutSection[];
}

export interface ResearchCatalogItem extends Omit<ApiResearchCatalogItem, 'tags' | 'riskFlags' | 'relatedExperiments'> {
  tags: string[];
  family: string;
  status: ResearchDecisionStatus;
  riskFlags: string[];
  relatedExperiments: string[];
}

export type ResearchRunReference = ApiResearchRunReference;

export interface ResearchCatalogResponse extends Omit<ApiResearchCatalogResponse, 'items'> {
  items: ResearchCatalogItem[];
}

export interface ResearchDetailResponse
  extends Omit<ApiResearchDetailResponse, 'summary' | 'outputTables' | 'availableRuns' | 'resultMetadata'> {
  summary: PublishedResearchSummary | null;
  outputTables: string[];
  availableRuns: ResearchRunReference[];
  resultMetadata: Record<string, unknown>;
}
