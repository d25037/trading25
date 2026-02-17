import yaml from 'js-yaml';

export interface GridParameterEntry {
  path: string;
  values: unknown[];
}

type RecordLike = Record<string, unknown>;

function isRecordLike(value: unknown): value is RecordLike {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function extractGridParameterEntries(content: string): GridParameterEntry[] {
  let parsed: unknown;
  try {
    parsed = yaml.load(content);
  } catch {
    return [];
  }

  if (!isRecordLike(parsed)) {
    return [];
  }

  const parameterRanges = parsed.parameter_ranges;
  if (!isRecordLike(parameterRanges)) {
    return [];
  }

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
