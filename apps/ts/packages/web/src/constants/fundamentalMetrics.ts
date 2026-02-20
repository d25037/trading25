export const FUNDAMENTAL_METRIC_IDS = [
  'per',
  'pbr',
  'roe',
  'roa',
  'eps',
  'bps',
  'dividendPerShare',
  'payoutRatio',
  'operatingMargin',
  'netMargin',
  'cashFlowOperating',
  'cashFlowInvesting',
  'cashFlowFinancing',
  'cashAndEquivalents',
  'fcf',
  'fcfYield',
  'fcfMargin',
  'cfoYield',
  'cfoMargin',
  'cfoToNetProfitRatio',
  'tradingValueToMarketCapRatio',
] as const;

export type FundamentalMetricId = (typeof FUNDAMENTAL_METRIC_IDS)[number];

export interface FundamentalMetricDefinition {
  id: FundamentalMetricId;
  label: string;
}

export const FUNDAMENTAL_METRIC_DEFINITIONS: FundamentalMetricDefinition[] = [
  { id: 'per', label: 'PER' },
  { id: 'pbr', label: 'PBR' },
  { id: 'roe', label: 'ROE' },
  { id: 'roa', label: 'ROA' },
  { id: 'eps', label: 'EPS' },
  { id: 'bps', label: 'BPS' },
  { id: 'dividendPerShare', label: '1株配当' },
  { id: 'payoutRatio', label: '配当性向' },
  { id: 'operatingMargin', label: '営業利益率' },
  { id: 'netMargin', label: '純利益率' },
  { id: 'cashFlowOperating', label: '営業CF' },
  { id: 'cashFlowInvesting', label: '投資CF' },
  { id: 'cashFlowFinancing', label: '財務CF' },
  { id: 'cashAndEquivalents', label: '現金' },
  { id: 'fcf', label: 'FCF' },
  { id: 'fcfYield', label: 'FCF利回り' },
  { id: 'fcfMargin', label: 'FCFマージン' },
  { id: 'cfoYield', label: 'CFO利回り' },
  { id: 'cfoMargin', label: 'CFOマージン' },
  { id: 'cfoToNetProfitRatio', label: '営業CF/純利益' },
  { id: 'tradingValueToMarketCapRatio', label: '時価総額/売買代金' },
];

export const DEFAULT_FUNDAMENTAL_METRIC_ORDER: FundamentalMetricId[] = [...FUNDAMENTAL_METRIC_IDS];

export const DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY: Record<FundamentalMetricId, boolean> = {
  per: true,
  pbr: true,
  roe: true,
  roa: true,
  eps: true,
  bps: true,
  dividendPerShare: true,
  payoutRatio: true,
  operatingMargin: true,
  netMargin: true,
  cashFlowOperating: true,
  cashFlowInvesting: true,
  cashFlowFinancing: true,
  cashAndEquivalents: true,
  fcf: true,
  fcfYield: true,
  fcfMargin: true,
  cfoYield: true,
  cfoMargin: true,
  cfoToNetProfitRatio: true,
  tradingValueToMarketCapRatio: true,
};

const FUNDAMENTAL_METRIC_ID_SET = new Set<FundamentalMetricId>(FUNDAMENTAL_METRIC_IDS);

export function isFundamentalMetricId(value: unknown): value is FundamentalMetricId {
  return typeof value === 'string' && FUNDAMENTAL_METRIC_ID_SET.has(value as FundamentalMetricId);
}

export function normalizeFundamentalMetricOrder(value: unknown): FundamentalMetricId[] {
  const normalizedOrder: FundamentalMetricId[] = [];
  const seen = new Set<FundamentalMetricId>();

  if (Array.isArray(value)) {
    for (const metricId of value) {
      if (!isFundamentalMetricId(metricId) || seen.has(metricId)) continue;
      seen.add(metricId);
      normalizedOrder.push(metricId);
    }
  }

  for (const metricId of DEFAULT_FUNDAMENTAL_METRIC_ORDER) {
    if (seen.has(metricId)) continue;
    normalizedOrder.push(metricId);
  }

  return normalizedOrder;
}

export function normalizeFundamentalMetricVisibility(value: unknown): Record<FundamentalMetricId, boolean> {
  const normalized = { ...DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY };
  if (typeof value !== 'object' || value === null) return normalized;

  for (const metricId of FUNDAMENTAL_METRIC_IDS) {
    const raw = (value as Record<string, unknown>)[metricId];
    if (typeof raw === 'boolean') {
      normalized[metricId] = raw;
    }
  }

  return normalized;
}

export function countVisibleFundamentalMetrics(
  metricOrder: FundamentalMetricId[],
  metricVisibility: Record<FundamentalMetricId, boolean>
): number {
  return metricOrder.filter((metricId) => metricVisibility[metricId]).length;
}

export const FUNDAMENTALS_METRIC_GRID_COLUMNS = 8;
export const FUNDAMENTALS_PANEL_BASE_HEIGHT_PX = 220;
export const FUNDAMENTALS_PANEL_ROW_HEIGHT_PX = 74;

export function resolveFundamentalsPanelHeightPx(visibleMetricCount: number): number {
  const rows = Math.max(1, Math.ceil(visibleMetricCount / FUNDAMENTALS_METRIC_GRID_COLUMNS));
  return FUNDAMENTALS_PANEL_BASE_HEIGHT_PX + rows * FUNDAMENTALS_PANEL_ROW_HEIGHT_PX;
}
