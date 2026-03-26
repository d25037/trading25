import { parseYamlValue } from './yamlUtils';

export interface GridParameterEntry {
  path: string;
  values: unknown[];
}

export interface GridValidationIssue {
  path: string;
  message: string;
}

export interface GridParameterAnalysis {
  parseError: string | null;
  hasParameterRanges: boolean;
  entries: GridParameterEntry[];
  paramCount: number;
  combinations: number;
  valid: boolean;
  readyToRun: boolean;
  errors: GridValidationIssue[];
  warnings: GridValidationIssue[];
}

type RecordLike = Record<string, unknown>;

function isRecordLike(value: unknown): value is RecordLike {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function joinPath(parts: string[]): string {
  return parts.join('.');
}

function validateSignalParams(
  node: RecordLike,
  prefix: string[],
  entries: GridParameterEntry[],
  errors: GridValidationIssue[],
  warnings: GridValidationIssue[]
) {
  for (const [key, value] of Object.entries(node)) {
    const currentPath = [...prefix, key];

    if (value === null) {
      warnings.push({
        path: joinPath(currentPath),
        message: 'Parameter is null and will be ignored. Use a candidate list such as [10, 20, 30].',
      });
      continue;
    }

    if (Array.isArray(value)) {
      entries.push({
        path: joinPath(currentPath),
        values: value,
      });
      if (value.length === 0) {
        warnings.push({
          path: joinPath(currentPath),
          message: 'Candidate list is empty. Optimization cannot run until at least one value is provided.',
        });
      }
      continue;
    }

    if (isRecordLike(value)) {
      validateSignalParams(value, currentPath, entries, errors, warnings);
      continue;
    }

    errors.push({
      path: joinPath(currentPath),
      message: 'Parameter must be a candidate list such as [10, 20, 30], not a scalar value.',
    });
  }
}

function analyzeParameterRanges(
  parameterRanges: RecordLike
): Pick<GridParameterAnalysis, 'entries' | 'errors' | 'warnings'> {
  const entries: GridParameterEntry[] = [];
  const errors: GridValidationIssue[] = [];
  const warnings: GridValidationIssue[] = [];

  for (const [sectionName, sectionValue] of Object.entries(parameterRanges)) {
    const sectionPath = ['parameter_ranges', sectionName];
    if (sectionValue === null) {
      continue;
    }
    if (!isRecordLike(sectionValue)) {
      errors.push({
        path: joinPath(sectionPath),
        message: 'Section must be a mapping of signal names to parameter maps.',
      });
      continue;
    }

    for (const [signalName, signalValue] of Object.entries(sectionValue)) {
      const signalPath = [...sectionPath, signalName];
      if (signalValue === null) {
        continue;
      }
      if (!isRecordLike(signalValue)) {
        errors.push({
          path: joinPath(signalPath),
          message: 'Signal must be a mapping of parameter names to candidate lists.',
        });
        continue;
      }
      validateSignalParams(signalValue, signalPath, entries, errors, warnings);
    }
  }

  if (entries.length === 0) {
    warnings.push({
      path: 'parameter_ranges',
      message: 'No parameter arrays found under "parameter_ranges". Add list values such as period: [10, 20, 30].',
    });
  }

  return { entries, errors, warnings };
}

export function analyzeGridParameters(content: string): GridParameterAnalysis {
  const parsed = parseYamlValue(content);
  if (parsed.error) {
    return {
      parseError: parsed.error,
      hasParameterRanges: false,
      entries: [],
      paramCount: 0,
      combinations: 0,
      valid: false,
      readyToRun: false,
      errors: [],
      warnings: [],
    };
  }

  if (!isRecordLike(parsed.value)) {
    return {
      parseError: null,
      hasParameterRanges: false,
      entries: [],
      paramCount: 0,
      combinations: 0,
      valid: false,
      readyToRun: false,
      errors: [
        {
          path: '$',
          message: 'Grid YAML root must be a mapping.',
        },
      ],
      warnings: [],
    };
  }

  const parameterRanges = parsed.value.parameter_ranges;
  if (!isRecordLike(parameterRanges)) {
    return {
      parseError: null,
      hasParameterRanges: false,
      entries: [],
      paramCount: 0,
      combinations: 0,
      valid: true,
      readyToRun: false,
      errors: [],
      warnings: [
        {
          path: 'parameter_ranges',
          message: 'Missing "parameter_ranges" key. Add sections such as entry_filter_params / exit_trigger_params.',
        },
      ],
    };
  }

  const { entries, errors, warnings } = analyzeParameterRanges(parameterRanges);
  const combinations = entries.length > 0 ? entries.reduce((acc, entry) => acc * entry.values.length, 1) : 0;

  return {
    parseError: null,
    hasParameterRanges: true,
    entries,
    paramCount: entries.length,
    combinations,
    valid: errors.length === 0,
    readyToRun: errors.length === 0 && entries.length > 0 && combinations > 0,
    errors,
    warnings,
  };
}

export function extractGridParameterEntries(content: string): GridParameterEntry[] {
  return analyzeGridParameters(content).entries;
}

export function formatGridParameterValue(value: unknown): string {
  if (value === undefined) {
    return 'undefined';
  }

  if (typeof value === 'string') {
    return `"${value}"`;
  }

  if (typeof value === 'number' || typeof value === 'boolean' || value === null) {
    return String(value);
  }

  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}
