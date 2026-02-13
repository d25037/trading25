import yaml from 'js-yaml';
import type { SignalDefinition, SignalFieldDefinition } from '@/types/backtest';

export interface StrategyValidationResult {
  valid: boolean;
  errors: string[];
  warnings: string[];
}

const ALLOWED_SHARED_CONFIG_KEYS = new Set([
  'initial_cash',
  'fees',
  'slippage',
  'spread',
  'borrow_fee',
  'max_concurrent_positions',
  'max_exposure',
  'start_date',
  'end_date',
  'dataset',
  'include_margin_data',
  'include_statements_data',
  'relative_mode',
  'benchmark_table',
  'group_by',
  'cash_sharing',
  'printlog',
  'stock_codes',
  'direction',
  'kelly_fraction',
  'min_allocation',
  'max_allocation',
  'parameter_optimization',
  'walk_forward',
  'timeframe',
]);

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isNumericConstraint(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

type FundamentalSpec = {
  childFieldMap: Map<string, Map<string, SignalFieldDefinition>>;
  parentFieldMap: Map<string, SignalFieldDefinition>;
};

const FUNDAMENTAL_PARENT_FIELD_FALLBACK = ['enabled', 'period_type', 'use_adjusted'];

function extractFundamentalParentFieldNames(fundamentalDefs: SignalDefinition[]): string[] {
  let inferredParentFields: Set<string> | null = null;

  for (const signal of fundamentalDefs) {
    try {
      const parsed = yaml.load(signal.yaml_snippet);
      const parsedRecord = isPlainObject(parsed) ? parsed : null;
      const snippetRoot =
        parsedRecord && isPlainObject(parsedRecord.fundamental)
          ? parsedRecord.fundamental
          : null;
      const childKey = signal.key.replace(/^fundamental_/, '');
      const currentParentFields = new Set(
        snippetRoot ? Object.keys(snippetRoot).filter((key) => key !== childKey) : []
      );

      if (currentParentFields.size === 0) {
        continue;
      }
      if (!inferredParentFields) {
        inferredParentFields = currentParentFields;
        continue;
      }
      inferredParentFields = new Set(
        [...inferredParentFields].filter((fieldName) => currentParentFields.has(fieldName))
      );
    } catch {
      // Ignore snippet parse failures and fall back to known parent fields.
    }
  }

  if (inferredParentFields && inferredParentFields.size > 0) {
    return [...inferredParentFields];
  }

  return FUNDAMENTAL_PARENT_FIELD_FALLBACK.filter((fieldName) =>
    fundamentalDefs.every((signal) => signal.fields.some((field) => field.name === fieldName))
  );
}

function buildFundamentalSpec(signalDefinitions: SignalDefinition[]): FundamentalSpec {
  const fundamentalDefs = signalDefinitions.filter((signal) => signal.key.startsWith('fundamental_'));
  const childFieldMaps = fundamentalDefs.map((signal) => new Map(signal.fields.map((field) => [field.name, field])));

  const parentFieldNames = extractFundamentalParentFieldNames(fundamentalDefs);

  const parentFieldMap = new Map<string, SignalFieldDefinition>();
  const firstChildFieldMap = childFieldMaps[0];
  for (const fieldName of parentFieldNames) {
    const field = firstChildFieldMap?.get(fieldName);
    if (field) {
      parentFieldMap.set(fieldName, field);
    }
  }

  const childFieldMap = new Map<string, Map<string, SignalFieldDefinition>>();
  for (const signal of fundamentalDefs) {
    const rawChildKey = signal.key.replace(/^fundamental_/, '');
    const childFields = new Map(signal.fields.map((field) => [field.name, field]));
    for (const parentFieldName of parentFieldNames) {
      if (parentFieldName !== 'enabled') {
        childFields.delete(parentFieldName);
      }
    }
    childFieldMap.set(rawChildKey, childFields);
  }

  return {
    childFieldMap,
    parentFieldMap,
  };
}

function validateFundamentalSection(
  sectionName: 'entry_filter_params' | 'exit_trigger_params',
  signalKey: string,
  signalConfig: unknown,
  spec: FundamentalSpec,
  errors: string[]
): void {
  if (!isPlainObject(signalConfig)) {
    errors.push(`${sectionName}.${signalKey} must be an object`);
    return;
  }

  const allowedFundamentalKeys = new Set([...spec.parentFieldMap.keys(), ...spec.childFieldMap.keys()]);

  for (const [fundamentalKey, fundamentalValue] of Object.entries(signalConfig)) {
    if (!allowedFundamentalKeys.has(fundamentalKey)) {
      errors.push(`${sectionName}.${signalKey}.${fundamentalKey} is not a valid parameter name`);
      continue;
    }

    const parentFieldDef = spec.parentFieldMap.get(fundamentalKey);
    if (parentFieldDef) {
      validateFieldValue(sectionName, signalKey, parentFieldDef, fundamentalValue, errors);
      continue;
    }

    const childFieldDefs = spec.childFieldMap.get(fundamentalKey);
    if (!childFieldDefs) {
      errors.push(`${sectionName}.${signalKey}.${fundamentalKey} is not a valid parameter name`);
      continue;
    }

    if (!isPlainObject(fundamentalValue)) {
      errors.push(`${sectionName}.${signalKey}.${fundamentalKey} must be an object`);
      continue;
    }

    for (const [fieldName, fieldValue] of Object.entries(fundamentalValue)) {
      const childFieldDef = childFieldDefs.get(fieldName);
      if (!childFieldDef) {
        errors.push(`${sectionName}.${signalKey}.${fundamentalKey}.${fieldName} is not a valid parameter name`);
        continue;
      }
      validateFieldValue(sectionName, `${signalKey}.${fundamentalKey}`, childFieldDef, fieldValue, errors);
    }
  }
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: Validation logic needs explicit branch handling
function validateFieldValue(
  sectionName: string,
  signalKey: string,
  field: SignalFieldDefinition,
  value: unknown,
  errors: string[]
): void {
  const fieldPath = `${sectionName}.${signalKey}.${field.name}`;

  if (field.type === 'boolean' && typeof value !== 'boolean') {
    errors.push(`${fieldPath} must be a boolean`);
    return;
  }

  if (field.type === 'number') {
    if (typeof value !== 'number' || Number.isNaN(value)) {
      errors.push(`${fieldPath} must be a number`);
      return;
    }

    const constraints = field.constraints;
    const gt = constraints?.gt;
    const ge = constraints?.ge;
    const lt = constraints?.lt;
    const le = constraints?.le;

    if (isNumericConstraint(gt) && value <= gt) {
      errors.push(`${fieldPath} must be > ${gt}`);
    }
    if (isNumericConstraint(ge) && value < ge) {
      errors.push(`${fieldPath} must be >= ${ge}`);
    }
    if (isNumericConstraint(lt) && value >= lt) {
      errors.push(`${fieldPath} must be < ${lt}`);
    }
    if (isNumericConstraint(le) && value > le) {
      errors.push(`${fieldPath} must be <= ${le}`);
    }
    return;
  }

  if ((field.type === 'string' || field.type === 'select') && typeof value !== 'string') {
    errors.push(`${fieldPath} must be a string`);
    return;
  }

  if (field.type === 'select' && field.options && typeof value === 'string' && !field.options.includes(value)) {
    errors.push(`${fieldPath} must be one of: ${field.options.join(', ')}`);
  }
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: Validation logic needs explicit branch handling
function validateSignalSection(
  sectionName: 'entry_filter_params' | 'exit_trigger_params',
  sectionValue: unknown,
  signalDefinitions: SignalDefinition[],
  errors: string[]
): void {
  if (!isPlainObject(sectionValue)) {
    errors.push(`${sectionName} must be an object`);
    return;
  }

  // Signal reference未取得時はunknown signal誤検知を避ける
  if (signalDefinitions.length === 0) {
    return;
  }

  const signalMap = new Map(signalDefinitions.map((signal) => [signal.key, signal]));
  let fundamentalSpec: FundamentalSpec | null = null;

  for (const [signalKey, signalConfig] of Object.entries(sectionValue)) {
    // fundamental は子シグナル構造（per/roe/...)を持つため専用検証を行う
    if (signalKey === 'fundamental') {
      fundamentalSpec ??= buildFundamentalSpec(signalDefinitions);
      validateFundamentalSection(sectionName, signalKey, signalConfig, fundamentalSpec, errors);
      continue;
    }

    const signalDef = signalMap.get(signalKey);
    if (!signalDef) {
      errors.push(`${sectionName}.${signalKey} is not a valid signal name`);
      continue;
    }

    if (!isPlainObject(signalConfig)) {
      errors.push(`${sectionName}.${signalKey} must be an object`);
      continue;
    }
    const allowedFields = new Map(signalDef.fields.map((field) => [field.name, field]));

    for (const [paramName, paramValue] of Object.entries(signalConfig)) {
      const fieldDef = allowedFields.get(paramName);
      if (!fieldDef) {
        errors.push(`${sectionName}.${signalKey}.${paramName} is not a valid parameter name`);
        continue;
      }
      validateFieldValue(sectionName, signalKey, fieldDef, paramValue, errors);
    }
  }
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: Validation combines schema/key/type/range checks in one pass
/**
 * @deprecated Backend strict validation (`/api/strategies/{name}/validate`) is the source of truth.
 * Keep this only for temporary compatibility and tests.
 */
export function validateStrategyConfigLocally(
  config: Record<string, unknown>,
  signalDefinitions: SignalDefinition[]
): StrategyValidationResult {
  const errors: string[] = [];
  const warnings: string[] = [];

  const entryFilter = config.entry_filter_params;
  const exitTrigger = config.exit_trigger_params;

  if (!entryFilter && !exitTrigger) {
    errors.push('entry_filter_params or exit_trigger_params is required');
  }

  if (entryFilter !== undefined) {
    validateSignalSection('entry_filter_params', entryFilter, signalDefinitions, errors);
  }

  if (exitTrigger !== undefined) {
    validateSignalSection('exit_trigger_params', exitTrigger, signalDefinitions, errors);
  }

  if (config.shared_config !== undefined) {
    if (!isPlainObject(config.shared_config)) {
      errors.push('shared_config must be an object');
    } else {
      for (const key of Object.keys(config.shared_config)) {
        if (!ALLOWED_SHARED_CONFIG_KEYS.has(key)) {
          errors.push(`shared_config.${key} is not a valid parameter name`);
        }
      }

      const kelly = config.shared_config.kelly_fraction;
      if (kelly !== undefined) {
        if (typeof kelly !== 'number' || Number.isNaN(kelly)) {
          errors.push('shared_config.kelly_fraction must be a number');
        } else if (kelly < 0 || kelly > 2) {
          errors.push('shared_config.kelly_fraction must be between 0 and 2');
        }
      }
    }
  }

  if (signalDefinitions.length === 0) {
    warnings.push('Signal reference is unavailable, so parameter-name validation may be incomplete');
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
  };
}

/**
 * @deprecated Use backend validation response directly when possible.
 */
export function mergeValidationResults(
  ...results: Array<StrategyValidationResult | null | undefined>
): StrategyValidationResult | null {
  const actual = results.filter((result): result is StrategyValidationResult => !!result);
  if (actual.length === 0) return null;

  const errors = actual.flatMap((r) => r.errors);
  const warnings = actual.flatMap((r) => r.warnings);

  return {
    valid: errors.length === 0,
    errors,
    warnings,
  };
}
