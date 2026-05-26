import type {
  PublishedResearchSummaryContract,
  ResearchCatalogItemContract,
  ResearchCatalogResponseContract,
  ResearchDecisionStatus,
  ResearchDetailResponseContract,
  ResearchHighlightContract,
  ResearchHighlightTone,
  ResearchLabelValueContract,
  ResearchRunReferenceContract,
  ResearchTableHighlightContract,
} from '@trading25/contracts/types/api-response-types';

export type { ResearchDecisionStatus, ResearchHighlightTone };

// API contract aliases stay tied to the stable contracts package; normalized UI models below stay web-local.
export type ApiResearchLabelValue = ResearchLabelValueContract;
export type ApiResearchHighlight = ResearchHighlightContract;
export type ApiResearchTableHighlight = ResearchTableHighlightContract;
export type ApiPublishedResearchSummary = PublishedResearchSummaryContract;
export type ApiResearchCatalogItem = ResearchCatalogItemContract;
export type ApiResearchRunReference = ResearchRunReferenceContract;
export type ApiResearchCatalogResponse = ResearchCatalogResponseContract;
export type ApiResearchDetailResponse = ResearchDetailResponseContract;

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
