import type { SignalDefinition } from '@/types/backtest';

export function formatConstraints(constraints: SignalDefinition['fields'][number]['constraints']): string[] {
  if (!constraints) return [];

  const parts: string[] = [];
  if (typeof constraints.gt === 'number') parts.push(`>${constraints.gt}`);
  if (typeof constraints.ge === 'number') parts.push(`>=${constraints.ge}`);
  if (typeof constraints.lt === 'number') parts.push(`<${constraints.lt}`);
  if (typeof constraints.le === 'number') parts.push(`<=${constraints.le}`);
  return parts;
}
