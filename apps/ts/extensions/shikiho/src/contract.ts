export const SHIKIHO_BRIDGE_CHANNEL = 'trading25.shikiho.v1';

const MAX_SNAPSHOT_BYTES = 64 * 1024;
const MAX_STRING_LENGTH = 4096;
const MAX_LIST_LENGTH = 100;
const MAX_REQUEST_ID_LENGTH = 256;
const SHIKIHO_HOST = 'shikiho.toyokeizai.net';
const ISO_TIMESTAMP = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|[+-](\d{2}):(\d{2}))$/;

export interface ShikihoQuoteV1 {
  tradingDate: string;
  observedAt: string;
  delayMinutes: 15;
  currentPrice: number;
  open: number;
  high: number;
  low: number;
  previousClose: number;
  volume: number | null;
  openTime: string | null;
  highTime: string | null;
  lowTime: string | null;
  sourceLabel: '会社四季報オンライン';
}

export interface ShikihoSnapshotV1 {
  schemaVersion: 1;
  extractorVersion: string;
  code: string;
  companyName: string | null;
  sourceUrl: string;
  capturedAt: string;
  pageUpdatedAt: string | null;
  editionLabel: string | null;
  contentHash: string;
  status: 'captured' | 'partial';
  features: string | null;
  consolidatedBusinesses: string | null;
  commentary: Array<{ heading: string | null; body: string }>;
  score: {
    overall: number | null;
    growth: number | null;
    profitability: number | null;
    safety: number | null;
    scale: number | null;
    value: number | null;
    priceMomentum: number | null;
  };
  comparisonCompanies: Array<{ code: string | null; name: string }>;
  industries: string[];
  marketThemes: string[];
  profile: Array<{ label: string; value: string }>;
  quote?: ShikihoQuoteV1;
  missingFields: string[];
}

export interface ShikihoCaptureDiagnosticV1 {
  schemaVersion: 1;
  code: string;
  observedAt: string;
  status: 'login_required' | 'page_changed' | 'storage_error';
}

export type ShikihoBridgeRequestV1 =
  | { channel: typeof SHIKIHO_BRIDGE_CHANNEL; direction: 'page-to-extension'; type: 'ping'; requestId: string }
  | {
      channel: typeof SHIKIHO_BRIDGE_CHANNEL;
      direction: 'page-to-extension';
      type: 'get_snapshot';
      requestId: string;
      code: string;
      forceRefresh: boolean;
    };

export type ShikihoBridgeResponseV1 =
  | { channel: typeof SHIKIHO_BRIDGE_CHANNEL; direction: 'extension-to-page'; type: 'ready'; requestId: string }
  | {
      channel: typeof SHIKIHO_BRIDGE_CHANNEL;
      direction: 'extension-to-page';
      type: 'snapshot';
      requestId: string;
      code: string;
      snapshot: ShikihoSnapshotV1 | null;
      diagnostic: ShikihoCaptureDiagnosticV1 | null;
    };

type UnknownRecord = Record<string, unknown>;

function isRecord(value: unknown): value is UnknownRecord {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isLimitedString(value: unknown, maxLength = MAX_STRING_LENGTH): value is string {
  return typeof value === 'string' && value.length > 0 && value.length <= maxLength;
}

function isNullableLimitedString(value: unknown): value is string | null {
  return value === null || isLimitedString(value);
}

function isIsoTimestamp(value: unknown): value is string {
  if (!isLimitedString(value, 64)) return false;
  const match = ISO_TIMESTAMP.exec(value);
  if (match === null) return false;

  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6]);
  const offsetHour = match[7] === undefined ? 0 : Number(match[7]);
  const offsetMinute = match[8] === undefined ? 0 : Number(match[8]);
  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const daysInMonth = [31, leapYear ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

  return (
    month >= 1 &&
    month <= 12 &&
    day >= 1 &&
    day <= (daysInMonth[month - 1] ?? 0) &&
    hour <= 23 &&
    minute <= 59 &&
    second <= 59 &&
    offsetHour <= 23 &&
    offsetMinute <= 59
  );
}

function isCalendarDate(value: unknown): value is string {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  return isIsoTimestamp(`${value}T00:00:00Z`);
}

function isExactCode(value: unknown): value is string {
  return typeof value === 'string' && /^\d{4}$/.test(value);
}

function isLimitedArray<T>(value: unknown, validateItem: (item: unknown) => item is T): value is T[] {
  return Array.isArray(value) && value.length <= MAX_LIST_LENGTH && value.every(validateItem);
}

function isScore(value: unknown): value is number | null {
  return value === null || (typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= 5);
}

function hasValidSource(value: unknown, code: string): value is string {
  if (!isLimitedString(value, 2048)) return false;
  try {
    const url = new URL(value);
    return (
      url.protocol === 'https:' &&
      url.host === SHIKIHO_HOST &&
      url.username === '' &&
      url.password === '' &&
      url.pathname === `/stocks/${code}`
    );
  } catch {
    return false;
  }
}

function isWithinSnapshotLimit(value: unknown): boolean {
  try {
    return new TextEncoder().encode(JSON.stringify(value)).byteLength <= MAX_SNAPSHOT_BYTES;
  } catch {
    return false;
  }
}

function isScoreRecord(value: unknown): value is ShikihoSnapshotV1['score'] {
  return (
    isRecord(value) &&
    isScore(value.overall) &&
    isScore(value.growth) &&
    isScore(value.profitability) &&
    isScore(value.safety) &&
    isScore(value.scale) &&
    isScore(value.value) &&
    isScore(value.priceMomentum)
  );
}

function hasExactKeys(value: UnknownRecord, expectedKeys: readonly string[]): boolean {
  const keys = Object.keys(value).sort();
  return keys.length === expectedKeys.length && keys.every((key, index) => key === expectedKeys[index]);
}

function isPositiveFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value > 0;
}

function isNullableQuoteTime(value: unknown): value is string | null {
  return value === null || (typeof value === 'string' && /^(?:[01]\d|2[0-3]):[0-5]\d$/.test(value));
}

function isQuoteRecord(value: unknown): value is ShikihoQuoteV1 {
  if (!isRecord(value)) return false;
  const expectedKeys = [
    'currentPrice',
    'delayMinutes',
    'high',
    'highTime',
    'low',
    'lowTime',
    'observedAt',
    'open',
    'openTime',
    'previousClose',
    'sourceLabel',
    'tradingDate',
    'volume',
  ];
  if (!hasExactKeys(value, expectedKeys)) return false;
  if (
    !isCalendarDate(value.tradingDate) ||
    !isIsoTimestamp(value.observedAt) ||
    !value.observedAt.startsWith(`${value.tradingDate}T`) ||
    value.delayMinutes !== 15 ||
    !isPositiveFiniteNumber(value.currentPrice) ||
    !isPositiveFiniteNumber(value.open) ||
    !isPositiveFiniteNumber(value.high) ||
    !isPositiveFiniteNumber(value.low) ||
    !isPositiveFiniteNumber(value.previousClose) ||
    !(
      value.volume === null ||
      (typeof value.volume === 'number' && Number.isFinite(value.volume) && value.volume >= 0)
    ) ||
    !isNullableQuoteTime(value.openTime) ||
    !isNullableQuoteTime(value.highTime) ||
    !isNullableQuoteTime(value.lowTime) ||
    value.sourceLabel !== '会社四季報オンライン'
  ) {
    return false;
  }
  return (
    value.low <= value.currentPrice &&
    value.currentPrice <= value.high &&
    value.low <= value.open &&
    value.open <= value.high
  );
}

function isCommentaryItem(value: unknown): value is ShikihoSnapshotV1['commentary'][number] {
  return isRecord(value) && isNullableLimitedString(value.heading) && isLimitedString(value.body);
}

function isComparisonCompany(value: unknown): value is ShikihoSnapshotV1['comparisonCompanies'][number] {
  return isRecord(value) && (value.code === null || isExactCode(value.code)) && isLimitedString(value.name);
}

function isProfileItem(value: unknown): value is ShikihoSnapshotV1['profile'][number] {
  return isRecord(value) && isLimitedString(value.label) && isLimitedString(value.value);
}

function isStringItem(value: unknown): value is string {
  return isLimitedString(value);
}

export function normalizeShikihoCode(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const normalized = value.trim();
  if (/^\d{4}$/.test(normalized)) return normalized;
  if (/^\d{4}0$/.test(normalized)) return normalized.slice(0, 4);
  return null;
}

export function parseShikihoSnapshot(value: unknown): ShikihoSnapshotV1 | null {
  if (!isRecord(value) || !isWithinSnapshotLimit(value) || !isExactCode(value.code)) return null;
  if (
    value.schemaVersion !== 1 ||
    !isLimitedString(value.extractorVersion, 64) ||
    !isNullableLimitedString(value.companyName) ||
    !hasValidSource(value.sourceUrl, value.code) ||
    !isIsoTimestamp(value.capturedAt) ||
    !(value.pageUpdatedAt === null || isIsoTimestamp(value.pageUpdatedAt)) ||
    !isNullableLimitedString(value.editionLabel) ||
    !isLimitedString(value.contentHash, 256) ||
    (value.status !== 'captured' && value.status !== 'partial') ||
    !isNullableLimitedString(value.features) ||
    !isNullableLimitedString(value.consolidatedBusinesses) ||
    !isLimitedArray(value.commentary, isCommentaryItem) ||
    !isScoreRecord(value.score) ||
    !isLimitedArray(value.comparisonCompanies, isComparisonCompany) ||
    !isLimitedArray(value.industries, isStringItem) ||
    !isLimitedArray(value.marketThemes, isStringItem) ||
    !isLimitedArray(value.profile, isProfileItem) ||
    !(value.quote === undefined || isQuoteRecord(value.quote)) ||
    !isLimitedArray(value.missingFields, isStringItem)
  ) {
    return null;
  }
  return value as unknown as ShikihoSnapshotV1;
}

export function parseShikihoDiagnostic(value: unknown): ShikihoCaptureDiagnosticV1 | null {
  if (
    !isRecord(value) ||
    value.schemaVersion !== 1 ||
    !isExactCode(value.code) ||
    !isIsoTimestamp(value.observedAt) ||
    (value.status !== 'login_required' && value.status !== 'page_changed' && value.status !== 'storage_error')
  ) {
    return null;
  }
  return value as unknown as ShikihoCaptureDiagnosticV1;
}

export function parseShikihoBridgeResponse(value: unknown): ShikihoBridgeResponseV1 | null {
  if (
    !isRecord(value) ||
    value.channel !== SHIKIHO_BRIDGE_CHANNEL ||
    value.direction !== 'extension-to-page' ||
    !isLimitedString(value.requestId, MAX_REQUEST_ID_LENGTH)
  ) {
    return null;
  }
  if (value.type === 'ready') return value as ShikihoBridgeResponseV1;
  if (value.type !== 'snapshot' || !isExactCode(value.code)) return null;

  const snapshot = value.snapshot === null ? null : parseShikihoSnapshot(value.snapshot);
  const diagnostic = value.diagnostic === null ? null : parseShikihoDiagnostic(value.diagnostic);
  if (
    (value.snapshot !== null && snapshot === null) ||
    (value.diagnostic !== null && diagnostic === null) ||
    (snapshot !== null && snapshot.code !== value.code) ||
    (diagnostic !== null && diagnostic.code !== value.code)
  ) {
    return null;
  }
  return { ...value, snapshot, diagnostic } as ShikihoBridgeResponseV1;
}
