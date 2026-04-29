import type { SignalDefinition, SignalFieldDefinition } from '@/types/backtest';
import { isPlainObject, parseYamlValue } from './yamlUtils';

export interface StrategyValidationResult {
  valid: boolean;
  errors: string[];
  warnings: string[];
}

type ExecutionPolicyMode =
  | 'standard'
  | 'next_session_round_trip'
  | 'current_session_round_trip'
  | 'overnight_round_trip';

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
  'data_source',
  'universe_preset',
  'universe_as_of_date',
  'universe_filters',
  'dataset_snapshot',
  'static_universe',
  'universe_provenance',
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
  'execution_policy',
]);

function isNumericConstraint(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

type FundamentalSpec = {
  childFieldMap: Map<string, Map<string, SignalFieldDefinition>>;
  parentFieldMap: Map<string, SignalFieldDefinition>;
};

const FUNDAMENTAL_PARENT_FIELD_FALLBACK = ['enabled', 'period_type', 'use_adjusted'];

function intersectFieldNames(left: Set<string>, right: Set<string>): Set<string> {
  return new Set([...left].filter((fieldName) => right.has(fieldName)));
}

function deriveFundamentalParentFields(signal: SignalDefinition): Set<string> {
  const parsed = parseYamlValue(signal.yaml_snippet);
  const parsedRecord = isPlainObject(parsed.value) ? parsed.value : null;
  const snippetRoot = parsedRecord && isPlainObject(parsedRecord.fundamental) ? parsedRecord.fundamental : null;
  const childKey = signal.key.replace(/^fundamental_/, '');
  return new Set(snippetRoot ? Object.keys(snippetRoot).filter((key) => key !== childKey) : []);
}

function resolveFallbackFundamentalParentFields(fundamentalDefs: SignalDefinition[]): string[] {
  return FUNDAMENTAL_PARENT_FIELD_FALLBACK.filter((fieldName) =>
    fundamentalDefs.every((signal) => signal.fields.some((field) => field.name === fieldName))
  );
}

function extractFundamentalParentFieldNames(fundamentalDefs: SignalDefinition[]): string[] {
  let inferredParentFields: Set<string> | null = null;

  for (const signal of fundamentalDefs) {
    const currentParentFields = deriveFundamentalParentFields(signal);
    if (currentParentFields.size === 0) {
      continue;
    }

    inferredParentFields = inferredParentFields
      ? intersectFieldNames(inferredParentFields, currentParentFields)
      : currentParentFields;
  }

  if (inferredParentFields?.size) {
    return Array.from(inferredParentFields);
  }

  return resolveFallbackFundamentalParentFields(fundamentalDefs);
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

function validateNumericFieldConstraints(
  fieldPath: string,
  value: number,
  field: SignalFieldDefinition,
  errors: string[]
): void {
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
}

function validateFieldValue(
  sectionName: string,
  signalKey: string,
  field: SignalFieldDefinition,
  value: unknown,
  errors: string[]
): void {
  const fieldPath = `${sectionName}.${signalKey}.${field.name}`;

  switch (field.type) {
    case 'boolean':
      if (typeof value !== 'boolean') {
        errors.push(`${fieldPath} must be a boolean`);
      }
      return;
    case 'number':
      if (typeof value !== 'number' || Number.isNaN(value)) {
        errors.push(`${fieldPath} must be a number`);
        return;
      }
      validateNumericFieldConstraints(fieldPath, value, field, errors);
      return;
    case 'string':
    case 'select':
      if (typeof value !== 'string') {
        errors.push(`${fieldPath} must be a string`);
        return;
      }
      if (field.type === 'select' && field.options && !field.options.includes(value)) {
        errors.push(`${fieldPath} must be one of: ${field.options.join(', ')}`);
      }
      return;
    default:
      return;
  }
}

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

function validateSharedConfig(sharedConfig: unknown, errors: string[]): void {
  if (!isPlainObject(sharedConfig)) {
    errors.push('shared_config must be an object');
    return;
  }

  for (const key of Object.keys(sharedConfig)) {
    if (!ALLOWED_SHARED_CONFIG_KEYS.has(key)) {
      errors.push(`shared_config.${key} is not a valid parameter name`);
    }
  }

  if ('dataset' in sharedConfig) {
    errors.push(
      "shared_config.dataset is no longer supported; use shared_config.universe_preset for PIT market universes, or shared_config.dataset_snapshot with data_source='dataset_snapshot' and static_universe=true for archived reproducibility."
    );
  }

  const dataSource = sharedConfig.data_source ?? 'market';
  if (dataSource !== 'market' && dataSource !== 'dataset_snapshot') {
    errors.push('shared_config.data_source must be one of: market, dataset_snapshot');
  }

  const universePreset = sharedConfig.universe_preset;
  const datasetSnapshot = sharedConfig.dataset_snapshot;
  const staticUniverse = sharedConfig.static_universe;
  const stockCodes = sharedConfig.stock_codes;
  const usesAllStocks =
    stockCodes === undefined || (Array.isArray(stockCodes) && stockCodes.length === 1 && stockCodes[0] === 'all');

  if (dataSource === 'market') {
    if (typeof datasetSnapshot === 'string' && datasetSnapshot.trim().length > 0) {
      errors.push("shared_config.dataset_snapshot is only allowed when shared_config.data_source is 'dataset_snapshot'");
    }
    if (usesAllStocks && (typeof universePreset !== 'string' || universePreset.trim().length === 0)) {
      errors.push(
        "shared_config.universe_preset is required for market-backed backtest YAML when stock_codes is ['all'] or omitted."
      );
    }
  }

  if (dataSource === 'dataset_snapshot') {
    if (typeof datasetSnapshot !== 'string' || datasetSnapshot.trim().length === 0) {
      errors.push("shared_config.dataset_snapshot is required when shared_config.data_source is 'dataset_snapshot'");
    }
    if (staticUniverse !== true) {
      errors.push("shared_config.static_universe must be true when shared_config.data_source is 'dataset_snapshot'");
    }
  }

  const kelly = sharedConfig.kelly_fraction;
  if (kelly === undefined) {
    return;
  }

  if (typeof kelly !== 'number' || Number.isNaN(kelly)) {
    errors.push('shared_config.kelly_fraction must be a number');
    return;
  }

  if (kelly < 0 || kelly > 2) {
    errors.push('shared_config.kelly_fraction must be between 0 and 2');
  }
}

function resolveExecutionPolicyMode(
  sharedConfig: Record<string, unknown>,
  errors: string[]
): ExecutionPolicyMode | null {
  const executionPolicy = sharedConfig.execution_policy;
  if (executionPolicy === undefined) {
    return 'standard';
  }
  if (!isPlainObject(executionPolicy)) {
    errors.push('shared_config.execution_policy must be an object');
    return null;
  }

  for (const key of Object.keys(executionPolicy)) {
    if (key !== 'mode') {
      errors.push(`shared_config.execution_policy.${key} is not a valid parameter name`);
    }
  }

  const mode = executionPolicy.mode;
  if (mode === undefined) {
    return 'standard';
  }
  if (typeof mode !== 'string') {
    errors.push('shared_config.execution_policy.mode must be a string');
    return null;
  }

  if (
    mode !== 'standard' &&
    mode !== 'next_session_round_trip' &&
    mode !== 'current_session_round_trip' &&
    mode !== 'overnight_round_trip'
  ) {
    errors.push(
      'shared_config.execution_policy.mode must be one of: standard, next_session_round_trip, current_session_round_trip, overnight_round_trip'
    );
    return null;
  }

  return mode;
}

function hasConfiguredExitTriggerParams(exitTrigger: unknown): boolean {
  if (exitTrigger === undefined) {
    return false;
  }
  if (!isPlainObject(exitTrigger)) {
    return true;
  }
  return Object.keys(exitTrigger).length > 0;
}

function validateRoundTripRules(config: Record<string, unknown>, errors: string[]): void {
  const sharedConfig = config.shared_config;
  if (!isPlainObject(sharedConfig)) {
    return;
  }

  const mode = resolveExecutionPolicyMode(sharedConfig, errors);
  if (mode === null || mode === 'standard') {
    return;
  }

  if (sharedConfig.timeframe !== undefined && sharedConfig.timeframe !== 'daily') {
    errors.push(`${mode} requires timeframe='daily'`);
  }

  if (hasConfiguredExitTriggerParams(config.exit_trigger_params)) {
    errors.push(`exit_trigger_params must be empty when shared_config.execution_policy.mode is '${mode}'`);
  }
}

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

  if (entryFilter === undefined && exitTrigger === undefined) {
    errors.push('entry_filter_params or exit_trigger_params is required');
  }

  if (entryFilter !== undefined) {
    validateSignalSection('entry_filter_params', entryFilter, signalDefinitions, errors);
  }

  if (exitTrigger !== undefined) {
    validateSignalSection('exit_trigger_params', exitTrigger, signalDefinitions, errors);
  }

  if (config.shared_config !== undefined) {
    validateSharedConfig(config.shared_config, errors);
  }

  validateRoundTripRules(config, errors);

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
