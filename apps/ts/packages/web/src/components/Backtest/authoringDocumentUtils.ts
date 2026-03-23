import type { SignalDefinition } from '@/types/backtest';
import { isPlainObject, parseYamlObject } from './yamlUtils';

const DEFAULT_FUNDAMENTAL_PARENT_FIELDS = ['enabled', 'period_type', 'use_adjusted'];
const STRATEGY_SECTION_KEYS = ['entry_filter_params', 'exit_trigger_params'] as const;

type SectionObjectRequirement = {
  key: string;
  error: string;
};

type StrategySectionKey = (typeof STRATEGY_SECTION_KEYS)[number];

function validateObjectRequirements(
  source: Record<string, unknown>,
  requirements: SectionObjectRequirement[]
): string | null {
  for (const requirement of requirements) {
    if (requirement.key in source && !isPlainObject(source[requirement.key])) {
      return requirement.error;
    }
  }
  return null;
}

function intersectStringSets(left: Set<string> | null, right: Set<string>): Set<string> {
  if (!left) {
    return new Set(right);
  }
  return new Set(Array.from(right).filter((value) => left.has(value)));
}

function resolveFundamentalRoot(source: Record<string, unknown>): Record<string, unknown> | null {
  if (isPlainObject(source.fundamental)) {
    return source.fundamental;
  }

  for (const sectionKey of STRATEGY_SECTION_KEYS) {
    const section = source[sectionKey];
    if (isPlainObject(section) && isPlainObject(section.fundamental)) {
      return section.fundamental;
    }
  }

  return null;
}

function getFundamentalParentKeys(definition: SignalDefinition): Set<string> {
  const parsed = parseYamlObject(definition.yaml_snippet);
  const fundamentalRoot = parsed.value ? resolveFundamentalRoot(parsed.value) : null;
  if (!fundamentalRoot) {
    return new Set();
  }

  const childKey = definition.key.replace(/^fundamental_/, '');
  return new Set(Object.keys(fundamentalRoot).filter((key) => key !== childKey));
}

function collectFundamentalAdvancedOnlyPaths(
  paths: Set<string>,
  sectionKey: StrategySectionKey,
  signalValue: unknown,
  fundamentalDefinitionsByType: Map<string, SignalDefinition>,
  parentFieldNames: string[]
): void {
  if (!isPlainObject(signalValue)) {
    paths.add(`${sectionKey}.fundamental`);
    return;
  }

  for (const [childKey, childValue] of Object.entries(signalValue)) {
    if (parentFieldNames.includes(childKey)) {
      continue;
    }
    if (!fundamentalDefinitionsByType.has(childKey) || !isPlainObject(childValue)) {
      paths.add(`${sectionKey}.fundamental.${childKey}`);
    }
  }
}

function collectStrategySectionAdvancedOnlyPaths(
  paths: Set<string>,
  sectionKey: StrategySectionKey,
  config: Record<string, unknown>,
  definitionsByType: Map<string, SignalDefinition>,
  fundamentalDefinitionsByType: Map<string, SignalDefinition>,
  parentFieldNames: string[]
): void {
  const section = isPlainObject(config[sectionKey]) ? config[sectionKey] : {};
  for (const [signalKey, signalValue] of Object.entries(section)) {
    if (signalKey === 'fundamental') {
      collectFundamentalAdvancedOnlyPaths(
        paths,
        sectionKey,
        signalValue,
        fundamentalDefinitionsByType,
        parentFieldNames
      );
      continue;
    }

    if (!definitionsByType.has(signalKey) || !isPlainObject(signalValue)) {
      paths.add(`${sectionKey}.${signalKey}`);
    }
  }
}

export function canVisualizeStrategyConfig(config: Record<string, unknown>): string | null {
  return validateObjectRequirements(config, [
    {
      key: 'shared_config',
      error: 'shared_config must be an object to edit it in Visual mode.',
    },
    {
      key: 'entry_filter_params',
      error: 'entry_filter_params must be an object to edit it in Visual mode.',
    },
    {
      key: 'exit_trigger_params',
      error: 'exit_trigger_params must be an object to edit it in Visual mode.',
    },
  ]);
}

export function deriveFundamentalParentFieldNames(
  definitions: SignalDefinition[],
  fallbackFields: string[] = DEFAULT_FUNDAMENTAL_PARENT_FIELDS
): string[] {
  let inferred: Set<string> | null = null;

  for (const definition of definitions) {
    const current = getFundamentalParentKeys(definition);
    if (current.size === 0) {
      continue;
    }
    inferred = intersectStringSets(inferred, current);
  }

  if (inferred?.size) {
    return Array.from(inferred);
  }

  return fallbackFields.filter((fieldName) =>
    definitions.every((definition) => definition.fields.some((field) => field.name === fieldName))
  );
}

export function buildVisualAdvancedOnlyPaths(
  config: Record<string, unknown>,
  definitionsByType: Map<string, SignalDefinition>,
  fundamentalDefinitionsByType: Map<string, SignalDefinition>,
  parentFieldNames: string[],
  visualTopLevelKeys: ReadonlySet<string>
): string[] {
  const paths = new Set<string>();

  for (const key of Object.keys(config)) {
    if (!visualTopLevelKeys.has(key)) {
      paths.add(key);
    }
  }

  for (const sectionKey of STRATEGY_SECTION_KEYS) {
    collectStrategySectionAdvancedOnlyPaths(
      paths,
      sectionKey,
      config,
      definitionsByType,
      fundamentalDefinitionsByType,
      parentFieldNames
    );
  }
  return Array.from(paths).sort();
}

export function canVisualizeDefaultDocument(document: Record<string, unknown>): string | null {
  const defaultSection = document.default;
  if (!isPlainObject(defaultSection)) {
    return "default.yaml must contain a 'default' object to use Visual mode.";
  }

  const sectionError = validateObjectRequirements(defaultSection, [
    {
      key: 'execution',
      error: 'default.execution must be an object to use Visual mode.',
    },
    {
      key: 'parameters',
      error: 'default.parameters must be an object to use Visual mode.',
    },
  ]);
  if (sectionError) {
    return sectionError;
  }

  const parameters = isPlainObject(defaultSection.parameters) ? defaultSection.parameters : null;
  if (parameters && 'shared_config' in parameters && !isPlainObject(parameters.shared_config)) {
    return 'default.parameters.shared_config must be an object to use Visual mode.';
  }

  return null;
}

export function buildDefaultDocumentAdvancedOnlyPaths(document: Record<string, unknown>): string[] {
  const paths = new Set<string>();
  for (const key of Object.keys(document)) {
    if (key !== 'default') {
      paths.add(key);
    }
  }

  const defaultSection = isPlainObject(document.default) ? document.default : {};
  for (const key of Object.keys(defaultSection)) {
    if (key !== 'execution' && key !== 'parameters') {
      paths.add(`default.${key}`);
    }
  }

  const parameters = isPlainObject(defaultSection.parameters) ? defaultSection.parameters : {};
  for (const key of Object.keys(parameters)) {
    if (key !== 'shared_config') {
      paths.add(`default.parameters.${key}`);
    }
  }

  return Array.from(paths).sort();
}
