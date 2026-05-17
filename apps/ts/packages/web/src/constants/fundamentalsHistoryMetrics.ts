export const FUNDAMENTALS_HISTORY_METRIC_IDS = [
  'eps',
  'forecastEps',
  'netSales',
  'operatingProfit',
  'forecastOperatingProfit',
  'operatingMargin',
  'roe',
  'dividendPerShare',
  'bps',
  'cashFlowOperating',
  'forecastDividendPerShare',
  'payoutRatio',
  'forecastPayoutRatio',
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
  { id: 'netSales', label: '売上高' },
  { id: 'operatingProfit', label: '営業利益' },
  { id: 'forecastOperatingProfit', label: '予想営業利益' },
  { id: 'operatingMargin', label: '営業利益率' },
  { id: 'roe', label: 'ROE' },
  { id: 'dividendPerShare', label: '1株配当' },
  { id: 'bps', label: 'BPS' },
  { id: 'cashFlowOperating', label: '営業CF' },
  { id: 'forecastDividendPerShare', label: '予想1株配当' },
  { id: 'payoutRatio', label: '配当性向' },
  { id: 'forecastPayoutRatio', label: '予想配当性向' },
  { id: 'cashFlowInvesting', label: '投資CF' },
  { id: 'cashFlowFinancing', label: '財務CF' },
];

export const DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER: FundamentalsHistoryMetricId[] = [
  ...FUNDAMENTALS_HISTORY_METRIC_IDS,
];

export const DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY: Record<FundamentalsHistoryMetricId, boolean> = {
  eps: true,
  forecastEps: true,
  netSales: true,
  operatingProfit: true,
  forecastOperatingProfit: true,
  operatingMargin: true,
  roe: true,
  dividendPerShare: true,
  bps: false,
  cashFlowOperating: false,
  forecastDividendPerShare: false,
  payoutRatio: false,
  forecastPayoutRatio: false,
  cashFlowInvesting: false,
  cashFlowFinancing: false,
};

const LEGACY_DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER: FundamentalsHistoryMetricId[] = [
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
];

const OPTIONAL_COMPACT_FUNDAMENTALS_HISTORY_METRIC_IDS: FundamentalsHistoryMetricId[] = [
  'bps',
  'cashFlowOperating',
  'forecastDividendPerShare',
  'payoutRatio',
  'forecastPayoutRatio',
  'cashFlowInvesting',
  'cashFlowFinancing',
];

const RENAMED_FUNDAMENTALS_HISTORY_METRIC_IDS: Record<string, FundamentalsHistoryMetricId[]> = {
  operatingProfitMargin: ['operatingProfit', 'operatingMargin'],
};

const FUNDAMENTALS_HISTORY_METRIC_ID_SET = new Set<FundamentalsHistoryMetricId>(FUNDAMENTALS_HISTORY_METRIC_IDS);

export function isFundamentalsHistoryMetricId(value: unknown): value is FundamentalsHistoryMetricId {
  return typeof value === 'string' && FUNDAMENTALS_HISTORY_METRIC_ID_SET.has(value as FundamentalsHistoryMetricId);
}

function isLegacyDefaultFundamentalsHistoryMetricOrder(value: unknown): boolean {
  if (!Array.isArray(value) || value.length !== LEGACY_DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER.length) return false;
  return LEGACY_DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER.every((metricId, index) => value[index] === metricId);
}

function isLegacyDefaultFundamentalsHistoryMetricVisibility(value: unknown): boolean {
  if (typeof value !== 'object' || value === null) return false;
  const record = value as Record<string, unknown>;
  return LEGACY_DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER.every((metricId) => record[metricId] === true);
}

function appendMetricId(
  metricId: FundamentalsHistoryMetricId,
  normalizedOrder: FundamentalsHistoryMetricId[],
  seen: Set<FundamentalsHistoryMetricId>
): void {
  if (seen.has(metricId)) return;
  seen.add(metricId);
  normalizedOrder.push(metricId);
}

function appendNormalizedMetricId(
  value: unknown,
  normalizedOrder: FundamentalsHistoryMetricId[],
  seen: Set<FundamentalsHistoryMetricId>
): void {
  const renamedMetricIds = typeof value === 'string' ? RENAMED_FUNDAMENTALS_HISTORY_METRIC_IDS[value] : null;
  if (renamedMetricIds) {
    for (const renamedMetricId of renamedMetricIds) {
      appendMetricId(renamedMetricId, normalizedOrder, seen);
    }
    return;
  }

  if (isFundamentalsHistoryMetricId(value)) {
    appendMetricId(value, normalizedOrder, seen);
  }
}

export function normalizeFundamentalsHistoryMetricOrder(value: unknown): FundamentalsHistoryMetricId[] {
  if (isLegacyDefaultFundamentalsHistoryMetricOrder(value)) {
    return [...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER];
  }

  const normalizedOrder: FundamentalsHistoryMetricId[] = [];
  const seen = new Set<FundamentalsHistoryMetricId>();

  if (Array.isArray(value)) {
    for (const metricId of value) {
      appendNormalizedMetricId(metricId, normalizedOrder, seen);
    }
  }

  for (const metricId of DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER) {
    appendMetricId(metricId, normalizedOrder, seen);
  }

  return normalizedOrder;
}

export function normalizeFundamentalsHistoryMetricVisibility(
  value: unknown
): Record<FundamentalsHistoryMetricId, boolean> {
  const normalized = { ...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY };
  if (typeof value !== 'object' || value === null) return normalized;
  if (isLegacyDefaultFundamentalsHistoryMetricVisibility(value)) return normalized;

  for (const metricId of FUNDAMENTALS_HISTORY_METRIC_IDS) {
    const raw = (value as Record<string, unknown>)[metricId];
    if (typeof raw === 'boolean') {
      normalized[metricId] = raw;
    }
  }

  const legacyOperatingProfitMarginVisibility = (value as Record<string, unknown>).operatingProfitMargin;
  if (typeof legacyOperatingProfitMarginVisibility === 'boolean') {
    normalized.operatingProfit = legacyOperatingProfitMarginVisibility;
    normalized.operatingMargin = legacyOperatingProfitMarginVisibility;
  }

  const looksLikeWidePersistedDefault = OPTIONAL_COMPACT_FUNDAMENTALS_HISTORY_METRIC_IDS.every(
    (metricId) => normalized[metricId]
  );
  if (looksLikeWidePersistedDefault) {
    for (const metricId of OPTIONAL_COMPACT_FUNDAMENTALS_HISTORY_METRIC_IDS) {
      normalized[metricId] = DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY[metricId];
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
