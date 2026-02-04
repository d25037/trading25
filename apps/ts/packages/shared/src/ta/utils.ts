export function cleanNaNValues(values: number[]): number[] {
  const result: number[] = [];
  for (let i = 0; i < values.length; i++) {
    const value = values[i];
    result[i] = value === null || value === undefined || Number.isNaN(value) ? 0 : value;
  }
  return result;
}
