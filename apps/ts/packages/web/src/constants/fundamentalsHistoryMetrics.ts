export const FUNDAMENTALS_HISTORY_METRIC_IDS = [
  'eps',
  'forecastEps',
  'bps',
  'dividendPerShare',
  'forecastDividendPerShare',
  'payoutRatio',
  'forecastPayoutRatio',
  'roe',
  'cashFlowOperating',
  'cashFlowInvesting',
  'cashFlowFinancing',
] as const;

export type FundamentalsHistoryMetricId = (typeof FUNDAMENTALS_HISTORY_METRIC_IDS)[number];

export interface FundamentalsHistoryMetricDefinition {
  id: FundamentalsHistoryMetricId;
  label: string;
}

export const FUNDAMENTALS_HISTORY_METRIC_DEFINITIONS: FundamentalsHistoryMetricDefinition[] = [
  { id: 'eps', label: 'EPS' },
  { id: 'forecastEps', label: '来期予想EPS' },
  { id: 'bps', label: 'BPS' },
  { id: 'dividendPerShare', label: '1株配当' },
  { id: 'forecastDividendPerShare', label: '予想1株配当' },
  { id: 'payoutRatio', label: '配当性向' },
  { id: 'forecastPayoutRatio', label: '予想配当性向' },
  { id: 'roe', label: 'ROE' },
  { id: 'cashFlowOperating', label: '営業CF' },
  { id: 'cashFlowInvesting', label: '投資CF' },
  { id: 'cashFlowFinancing', label: '財務CF' },
];

export const DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER: FundamentalsHistoryMetricId[] = [
  ...FUNDAMENTALS_HISTORY_METRIC_IDS,
];

export const DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY: Record<FundamentalsHistoryMetricId, boolean> = {
  eps: true,
  forecastEps: true,
  bps: true,
  dividendPerShare: true,
  forecastDividendPerShare: true,
  payoutRatio: true,
  forecastPayoutRatio: true,
  roe: true,
  cashFlowOperating: true,
  cashFlowInvesting: true,
  cashFlowFinancing: true,
};

const FUNDAMENTALS_HISTORY_METRIC_ID_SET = new Set<FundamentalsHistoryMetricId>(FUNDAMENTALS_HISTORY_METRIC_IDS);

export function isFundamentalsHistoryMetricId(value: unknown): value is FundamentalsHistoryMetricId {
  return typeof value === 'string' && FUNDAMENTALS_HISTORY_METRIC_ID_SET.has(value as FundamentalsHistoryMetricId);
}

export function normalizeFundamentalsHistoryMetricOrder(value: unknown): FundamentalsHistoryMetricId[] {
  const normalizedOrder: FundamentalsHistoryMetricId[] = [];
  const seen = new Set<FundamentalsHistoryMetricId>();

  if (Array.isArray(value)) {
    for (const metricId of value) {
      if (!isFundamentalsHistoryMetricId(metricId) || seen.has(metricId)) continue;
      seen.add(metricId);
      normalizedOrder.push(metricId);
    }
  }

  for (const metricId of DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER) {
    if (seen.has(metricId)) continue;
    normalizedOrder.push(metricId);
  }

  return normalizedOrder;
}

export function normalizeFundamentalsHistoryMetricVisibility(
  value: unknown
): Record<FundamentalsHistoryMetricId, boolean> {
  const normalized = { ...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY };
  if (typeof value !== 'object' || value === null) return normalized;

  for (const metricId of FUNDAMENTALS_HISTORY_METRIC_IDS) {
    const raw = (value as Record<string, unknown>)[metricId];
    if (typeof raw === 'boolean') {
      normalized[metricId] = raw;
    }
  }

  return normalized;
}

export function countVisibleFundamentalsHistoryMetrics(
  metricOrder: FundamentalsHistoryMetricId[],
  metricVisibility: Record<FundamentalsHistoryMetricId, boolean>
): number {
  return metricOrder.filter((metricId) => metricVisibility[metricId]).length;
}
