import yaml from 'js-yaml';

export interface GridParameterEntry {
  path: string;
  values: unknown[];
}

export interface GridParameterAnalysis {
  parseError: string | null;
  hasParameterRanges: boolean;
  entries: GridParameterEntry[];
  paramCount: number;
  combinations: number;
}

type RecordLike = Record<string, unknown>;

function isRecordLike(value: unknown): value is RecordLike {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function extractEntriesFromRanges(parameterRanges: RecordLike): GridParameterEntry[] {
  const entries: GridParameterEntry[] = [];
  const walk = (node: RecordLike, prefix: string[]) => {
    for (const [key, value] of Object.entries(node)) {
      const currentPath = [...prefix, key];

      if (Array.isArray(value)) {
        entries.push({
          path: currentPath.join('.'),
          values: value,
        });
        continue;
      }

      if (isRecordLike(value)) {
        walk(value, currentPath);
      }
    }
  };

  walk(parameterRanges, []);
  return entries;
}

export function analyzeGridParameters(content: string): GridParameterAnalysis {
  let parsed: unknown;
  try {
    parsed = yaml.load(content);
  } catch (error) {
    return {
      parseError: `YAML parse error: ${error instanceof Error ? error.message : 'Unknown error'}`,
      hasParameterRanges: false,
      entries: [],
      paramCount: 0,
      combinations: 0,
    };
  }

  if (!isRecordLike(parsed)) {
    return {
      parseError: null,
      hasParameterRanges: false,
      entries: [],
      paramCount: 0,
      combinations: 0,
    };
  }

  const parameterRanges = parsed.parameter_ranges;
  if (!isRecordLike(parameterRanges)) {
    return {
      parseError: null,
      hasParameterRanges: false,
      entries: [],
      paramCount: 0,
      combinations: 0,
    };
  }

  const entries = extractEntriesFromRanges(parameterRanges);
  const combinations = entries.length > 0 ? entries.reduce((acc, entry) => acc * entry.values.length, 1) : 0;

  return {
    parseError: null,
    hasParameterRanges: true,
    entries,
    paramCount: entries.length,
    combinations,
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
