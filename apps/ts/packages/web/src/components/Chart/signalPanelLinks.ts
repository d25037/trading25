import type { SignalConfig } from '@/stores/chartStore';
import type { SignalDefinition } from '@/types/backtest';

export type SignalLinkedPanel =
  | 'ppo'
  | 'riskAdjustedReturn'
  | 'volumeComparison'
  | 'tradingValueMA'
  | 'fundamentals'
  | 'fundamentalsHistory'
  | 'marginPressure'
  | 'factorRegression';

export interface PanelSignalLink {
  signalTypes: string[];
  requirements: string[];
}

export type PanelSignalLinkMap = Record<SignalLinkedPanel, PanelSignalLink>;

interface BuildSignalPanelLinksInput {
  signals: SignalConfig[];
  definitions: SignalDefinition[];
}

const PANEL_KEYS: SignalLinkedPanel[] = [
  'ppo',
  'riskAdjustedReturn',
  'volumeComparison',
  'tradingValueMA',
  'fundamentals',
  'fundamentalsHistory',
  'marginPressure',
  'factorRegression',
];

function createEmptyLinkMapSets(): Record<SignalLinkedPanel, { signalTypes: Set<string>; requirements: Set<string> }> {
  return {
    ppo: { signalTypes: new Set<string>(), requirements: new Set<string>() },
    riskAdjustedReturn: { signalTypes: new Set<string>(), requirements: new Set<string>() },
    volumeComparison: { signalTypes: new Set<string>(), requirements: new Set<string>() },
    tradingValueMA: { signalTypes: new Set<string>(), requirements: new Set<string>() },
    fundamentals: { signalTypes: new Set<string>(), requirements: new Set<string>() },
    fundamentalsHistory: { signalTypes: new Set<string>(), requirements: new Set<string>() },
    marginPressure: { signalTypes: new Set<string>(), requirements: new Set<string>() },
    factorRegression: { signalTypes: new Set<string>(), requirements: new Set<string>() },
  };
}

function createEmptyLinkMap(): PanelSignalLinkMap {
  return {
    ppo: { signalTypes: [], requirements: [] },
    riskAdjustedReturn: { signalTypes: [], requirements: [] },
    volumeComparison: { signalTypes: [], requirements: [] },
    tradingValueMA: { signalTypes: [], requirements: [] },
    fundamentals: { signalTypes: [], requirements: [] },
    fundamentalsHistory: { signalTypes: [], requirements: [] },
    marginPressure: { signalTypes: [], requirements: [] },
    factorRegression: { signalTypes: [], requirements: [] },
  };
}

function isSignalDefinitionMatch(signalType: string, definitionKey: string): boolean {
  return definitionKey === signalType || definitionKey.endsWith(`_${signalType}`);
}

function findSignalDefinition(
  signalType: string,
  definitions: SignalDefinition[]
): SignalDefinition | null {
  for (const definition of definitions) {
    if (isSignalDefinitionMatch(signalType, definition.key)) {
      return definition;
    }
  }
  return null;
}

function resolvePanelsForRequirement(requirement: string): SignalLinkedPanel[] {
  if (requirement.startsWith('statements:')) {
    return ['fundamentals', 'fundamentalsHistory'];
  }

  if (requirement === 'margin') {
    return ['marginPressure'];
  }

  if (requirement === 'benchmark' || requirement === 'sector') {
    return ['factorRegression'];
  }

  if (requirement === 'volume') {
    return ['volumeComparison', 'tradingValueMA'];
  }

  if (requirement === 'ohlc') {
    return ['ppo', 'riskAdjustedReturn'];
  }

  return [];
}

function finalizeLinkMap(
  linkSets: Record<SignalLinkedPanel, { signalTypes: Set<string>; requirements: Set<string> }>
): PanelSignalLinkMap {
  const result = createEmptyLinkMap();

  for (const panel of PANEL_KEYS) {
    result[panel] = {
      signalTypes: [...linkSets[panel].signalTypes].sort((a, b) => a.localeCompare(b)),
      requirements: [...linkSets[panel].requirements].sort((a, b) => a.localeCompare(b)),
    };
  }

  return result;
}

export function buildSignalPanelLinks({ signals, definitions }: BuildSignalPanelLinksInput): PanelSignalLinkMap {
  if (!signals.length || !definitions.length) {
    return createEmptyLinkMap();
  }

  const linkSets = createEmptyLinkMapSets();

  for (const signal of signals) {
    if (!signal.enabled) continue;

    const definition = findSignalDefinition(signal.type, definitions);
    if (!definition) continue;

    for (const requirement of definition.data_requirements) {
      const targetPanels = resolvePanelsForRequirement(requirement);
      if (targetPanels.length === 0) continue;

      for (const panel of targetPanels) {
        linkSets[panel].signalTypes.add(signal.type);
        linkSets[panel].requirements.add(requirement);
      }
    }
  }

  return finalizeLinkMap(linkSets);
}
