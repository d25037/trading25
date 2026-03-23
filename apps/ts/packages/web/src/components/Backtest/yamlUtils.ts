import yaml from 'js-yaml';

export type YamlRecord = Record<string, unknown>;

export function isPlainObject(value: unknown): value is YamlRecord {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function parseYamlValue(content: string): { value: unknown; error: string | null } {
  try {
    return { value: yaml.load(content), error: null };
  } catch (error) {
    return {
      value: null,
      error: `YAML parse error: ${error instanceof Error ? error.message : 'Unknown error'}`,
    };
  }
}

export function parseYamlObject(content: string): { value: YamlRecord | null; error: string | null } {
  const parsed = parseYamlValue(content);
  if (parsed.error) {
    return { value: null, error: parsed.error };
  }

  if (!isPlainObject(parsed.value)) {
    return { value: null, error: 'Invalid YAML: Must be an object' };
  }

  return { value: parsed.value, error: null };
}

export function dumpYamlObject(value: YamlRecord): string {
  return yaml.dump(value, { indent: 2, lineWidth: 120 });
}

export function safeDumpYaml(value: YamlRecord): string {
  try {
    return dumpYamlObject(value);
  } catch {
    return JSON.stringify(value, null, 2);
  }
}
