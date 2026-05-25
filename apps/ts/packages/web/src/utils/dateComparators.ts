export function compareTimestampDesc(left: string, right: string): number {
  return Date.parse(right) - Date.parse(left);
}
