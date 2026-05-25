export function compareTimestampDesc(left: string, right: string): number {
  return Date.parse(right) - Date.parse(left);
}

export function compareOptionalTimestampDesc(
  left: string | null | undefined,
  right: string | null | undefined
): number {
  const leftTime = left ? Date.parse(left) : 0;
  const rightTime = right ? Date.parse(right) : 0;
  return rightTime - leftTime;
}
