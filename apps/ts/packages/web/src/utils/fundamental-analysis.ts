export interface FinancialDataInput {
  roe?: number | null;
  eps?: number | null;
  netProfit?: number | null;
  equity?: number | null;
}

export function isFiscalYear(periodType: string | null | undefined): boolean {
  return periodType === 'FY';
}

function isValidEps(eps: number | null | undefined): eps is number {
  return typeof eps === 'number' && Number.isFinite(eps) && eps !== 0;
}

export function hasActualFinancialData(data: FinancialDataInput): boolean {
  if (data.roe !== null && data.roe !== undefined) return true;
  if (isValidEps(data.eps)) return true;
  if (typeof data.netProfit === 'number') return true;
  if (typeof data.equity === 'number') return true;
  return false;
}
