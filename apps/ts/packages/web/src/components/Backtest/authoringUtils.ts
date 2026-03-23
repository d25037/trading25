import type { SignalCategory, SignalDefinition, SignalFieldDefinition } from '@/types/backtest';
import { isPlainObject } from './yamlUtils';

export { dumpYamlObject, isPlainObject, parseYamlObject, safeDumpYaml } from './yamlUtils';

export function getValueAtPath(source: Record<string, unknown>, path: string): unknown {
  const parts = path.split('.');
  let current: unknown = source;

  for (const part of parts) {
    if (!isPlainObject(current) || !(part in current)) {
      return undefined;
    }
    current = current[part];
  }

  return current;
}

export function normalizeSignalSection(value: unknown): Record<string, unknown> {
  return isPlainObject(value) ? value : {};
}

export function hasValueAtPath(source: Record<string, unknown>, path: string): boolean {
  const parts = path.split('.');
  let current: unknown = source;

  for (const part of parts) {
    if (!isPlainObject(current) || !(part in current)) {
      return false;
    }
    current = current[part];
  }

  return true;
}

function cloneBranch(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => cloneBranch(item));
  }
  if (isPlainObject(value)) {
    return Object.fromEntries(Object.entries(value).map(([key, child]) => [key, cloneBranch(child)]));
  }
  return value;
}

export function setValueAtPath(source: Record<string, unknown>, path: string, value: unknown): Record<string, unknown> {
  const parts = path.split('.');
  const next = cloneBranch(source) as Record<string, unknown>;
  let current: Record<string, unknown> = next;

  for (const [index, part] of parts.entries()) {
    const isLeaf = index === parts.length - 1;
    if (isLeaf) {
      current[part] = value;
      break;
    }

    const existing = current[part];
    if (!isPlainObject(existing)) {
      current[part] = {};
    } else {
      current[part] = cloneBranch(existing);
    }
    current = current[part] as Record<string, unknown>;
  }

  return next;
}

export function removeValueAtPath(source: Record<string, unknown>, path: string): Record<string, unknown> {
  const parts = path.split('.');
  const next = cloneBranch(source) as Record<string, unknown>;
  const parents: Array<{ parent: Record<string, unknown>; key: string }> = [];
  let current: Record<string, unknown> = next;

  for (const [index, part] of parts.entries()) {
    const isLeaf = index === parts.length - 1;
    if (isLeaf) {
      delete current[part];
      break;
    }

    const child = current[part];
    if (!isPlainObject(child)) {
      return next;
    }

    parents.push({ parent: current, key: part });
    current = child;
  }

  for (let index = parents.length - 1; index >= 0; index -= 1) {
    const { parent, key } = parents[index] ?? {};
    if (!parent || !key) continue;
    const child = parent[key];
    if (isPlainObject(child) && Object.keys(child).length === 0) {
      delete parent[key];
    }
  }

  return next;
}

export function asStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is string => typeof item === 'string');
}

export function coerceNumber(value: string): number | null {
  const trimmed = value.trim();
  if (!trimmed) return null;
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
}

export function getSignalFieldDefaultValue(field: SignalFieldDefinition): unknown {
  if (typeof field.default === 'boolean' || typeof field.default === 'number' || typeof field.default === 'string') {
    return field.default;
  }
  if (field.type === 'boolean') return false;
  if (field.type === 'number') return 0;
  return '';
}

export function buildSignalOptions(
  section: Record<string, unknown>,
  categories: SignalCategory[],
  regularDefinitions: SignalDefinition[],
  sectionKey: 'entry_filter_params' | 'exit_trigger_params'
): Array<{ category: SignalCategory; signals: SignalDefinition[] }> {
  const existing = new Set(Object.keys(section).filter((key) => key !== 'fundamental'));
  return categories
    .map((category) => ({
      category,
      signals: regularDefinitions.filter(
        (definition) =>
          definition.category === category.key &&
          !existing.has(definition.signal_type) &&
          (sectionKey === 'entry_filter_params' || !definition.exit_disabled)
      ),
    }))
    .filter((entry) => entry.signals.length > 0);
}

export function updateRegularSignalConfig(
  section: Record<string, unknown>,
  signalType: string,
  field: SignalFieldDefinition,
  value: unknown
): Record<string, unknown> {
  const currentSignal = normalizeSignalSection(section[signalType]);
  return {
    ...currentSignal,
    [field.name]: value,
  };
}

export function buildDefaultFundamentalConfig(fields: SignalFieldDefinition[]): Record<string, unknown> {
  return Object.fromEntries(fields.map((field) => [field.name, getSignalFieldDefaultValue(field)]));
}

function ensureFundamentalRoot(
  currentFundamental: Record<string, unknown>,
  defaultFundamentalConfig: Record<string, unknown>
): Record<string, unknown> {
  return Object.keys(currentFundamental).length > 0 ? currentFundamental : defaultFundamentalConfig;
}

export function addFundamentalSignalConfig(
  currentFundamental: Record<string, unknown>,
  childKey: string,
  definition: SignalDefinition,
  parentFieldNames: string[],
  defaultFundamentalConfig: Record<string, unknown>
): Record<string, unknown> {
  return {
    ...ensureFundamentalRoot(currentFundamental, defaultFundamentalConfig),
    [childKey]: Object.fromEntries(
      definition.fields
        .filter((field) => !parentFieldNames.includes(field.name) || field.name === 'enabled')
        .map((field) => [field.name, getSignalFieldDefaultValue(field)])
    ),
  };
}

export function updateFundamentalParentConfig(
  currentFundamental: Record<string, unknown>,
  field: SignalFieldDefinition,
  value: unknown,
  defaultFundamentalConfig: Record<string, unknown>
): Record<string, unknown> {
  return {
    ...ensureFundamentalRoot(currentFundamental, defaultFundamentalConfig),
    [field.name]: value,
  };
}

export function updateFundamentalChildConfig(
  currentFundamental: Record<string, unknown>,
  childKey: string,
  field: SignalFieldDefinition,
  value: unknown,
  defaultFundamentalConfig: Record<string, unknown>
): Record<string, unknown> {
  const currentChild = normalizeSignalSection(currentFundamental[childKey]);
  return {
    ...ensureFundamentalRoot(currentFundamental, defaultFundamentalConfig),
    [childKey]: {
      ...currentChild,
      [field.name]: value,
    },
  };
}

export function removeFundamentalChildConfig(
  currentFundamental: Record<string, unknown>,
  childKey: string,
  parentFieldNames: string[]
): { nextFundamental: Record<string, unknown>; shouldRemoveSection: boolean } {
  const nextFundamental = removeValueAtPath(currentFundamental, childKey);
  const remainingChildKeys = Object.keys(nextFundamental).filter((key) => !parentFieldNames.includes(key));
  return {
    nextFundamental,
    shouldRemoveSection: remainingChildKeys.length === 0,
  };
}
