export const SHIKIHO_BRIDGE_CHANNEL = 'trading25.shikiho.v1';

const MAX_SNAPSHOT_BYTES = 64 * 1024;
const MAX_STRING_LENGTH = 4096;
const MAX_LIST_LENGTH = 100;
const MAX_REQUEST_ID_LENGTH = 256;
const MAX_TRACE_NUMBER = Number.MAX_SAFE_INTEGER;
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

export type ShikihoTracePhase =
  | 'queued'
  | 'probing_tabs'
  | 'acquiring_tab'
  | 'waiting_receiver'
  | 'observing_dom'
  | 'core_partial'
  | 'core_ready'
  | 'settling'
  | 'saving'
  | 'complete'
  | 'timeout'
  | 'error';

export type ShikihoTraceOutcome = 'success' | 'partial' | 'login_required' | 'page_changed' | 'timeout' | 'error';

export type ShikihoTraceMode =
  | 'acquisition_unbound'
  | 'exact_user_tab'
  | 'new_owned_tab'
  | 'warm_owned_same_code'
  | 'warm_owned_navigated';

export type ShikihoWaitEndReason =
  | 'field_stable'
  | 'deadline'
  | 'login_confirmed'
  | 'navigation_changed'
  | 'invalid_response'
  | 'error';

export type ShikihoTraceField =
  | 'identity'
  | 'quote'
  | 'features'
  | 'consolidatedBusinesses'
  | 'commentary'
  | 'score'
  | 'comparisonCompanies'
  | 'industries'
  | 'marketThemes'
  | 'profile'
  | 'editionLabel'
  | 'pageUpdatedAt'
  | 'coreReady';

export interface ShikihoFieldMilestonesV1 {
  identity: number | null;
  quote: number | null;
  features: number | null;
  consolidatedBusinesses: number | null;
  commentary: number | null;
  score: number | null;
  comparisonCompanies: number | null;
  industries: number | null;
  marketThemes: number | null;
  profile: number | null;
  editionLabel: number | null;
  pageUpdatedAt: number | null;
  coreReady: number | null;
}

export interface ShikihoCaptureTraceV1 {
  schemaVersion: 1;
  attemptId: string;
  code: string;
  mode: ShikihoTraceMode;
  phase: ShikihoTracePhase;
  startedAt: string;
  updatedAt: string;
  outcome: ShikihoTraceOutcome | null;
  waitEndReason: ShikihoWaitEndReason | null;
  receiverAttempts: number;
  receiverReadyMs: number | null;
  documentReadyState: DocumentReadyState | null;
  navigation: {
    responseStartMs: number | null;
    domInteractiveMs: number | null;
    domContentLoadedMs: number | null;
    loadEndMs: number | null;
  };
  dom: {
    firstSampleMs: number | null;
    mutationBatches: number;
    meaningfulChanges: number;
    samples: number;
    presentFields: ShikihoTraceField[];
    missingFields: ShikihoTraceField[];
    firstSeenMs: ShikihoFieldMilestonesV1;
  };
  extraction: {
    samples: number;
    lastMs: number | null;
    maxMs: number | null;
    totalMs: number;
  };
  timings: {
    probeMs: number;
    acquisitionMs: number;
    receiverMs: number;
    domObservationMs: number;
    storageMs: number;
    totalMs: number;
  };
}

export interface ShikihoCaptureProgressV1 {
  schemaVersion: 1;
  attemptId: string;
  code: string;
  sequence: number;
  candidate: ShikihoSnapshotV1 | null;
  trace: ShikihoCaptureTraceV1;
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
      trace: ShikihoCaptureTraceV1 | null;
    }
  | {
      channel: typeof SHIKIHO_BRIDGE_CHANNEL;
      direction: 'extension-to-page';
      type: 'capture_progress';
      requestId: string;
      code: string;
      attemptId: string;
      sequence: number;
      candidate: ShikihoSnapshotV1 | null;
      trace: ShikihoCaptureTraceV1;
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
    offsetMinute <= 59 &&
    (offsetHour < 14 || (offsetHour === 14 && offsetMinute === 0))
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

function isBoundedNonnegativeNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= MAX_TRACE_NUMBER;
}

function isNonnegativeSafeInteger(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0;
}

function isPositiveSafeInteger(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) > 0;
}

function isNullableBoundedNonnegativeNumber(value: unknown): value is number | null {
  return value === null || isBoundedNonnegativeNumber(value);
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

const TRACE_FIELDS: readonly ShikihoTraceField[] = [
  'identity',
  'quote',
  'features',
  'consolidatedBusinesses',
  'commentary',
  'score',
  'comparisonCompanies',
  'industries',
  'marketThemes',
  'profile',
  'editionLabel',
  'pageUpdatedAt',
  'coreReady',
];

function isTraceField(value: unknown): value is ShikihoTraceField {
  return typeof value === 'string' && TRACE_FIELDS.includes(value as ShikihoTraceField);
}

function isUniqueTraceFieldArray(value: unknown): value is ShikihoTraceField[] {
  return isLimitedArray(value, isTraceField) && new Set(value).size === value.length;
}

function isTracePhase(value: unknown): value is ShikihoTracePhase {
  return (
    value === 'queued' ||
    value === 'probing_tabs' ||
    value === 'acquiring_tab' ||
    value === 'waiting_receiver' ||
    value === 'observing_dom' ||
    value === 'core_partial' ||
    value === 'core_ready' ||
    value === 'settling' ||
    value === 'saving' ||
    value === 'complete' ||
    value === 'timeout' ||
    value === 'error'
  );
}

function isTraceOutcome(value: unknown): value is ShikihoTraceOutcome | null {
  return (
    value === null ||
    value === 'success' ||
    value === 'partial' ||
    value === 'login_required' ||
    value === 'page_changed' ||
    value === 'timeout' ||
    value === 'error'
  );
}

function isTraceMode(value: unknown): value is ShikihoTraceMode {
  return (
    value === 'acquisition_unbound' ||
    value === 'exact_user_tab' ||
    value === 'new_owned_tab' ||
    value === 'warm_owned_same_code' ||
    value === 'warm_owned_navigated'
  );
}

function isWaitEndReason(value: unknown): value is ShikihoWaitEndReason | null {
  return (
    value === null ||
    value === 'field_stable' ||
    value === 'deadline' ||
    value === 'login_confirmed' ||
    value === 'navigation_changed' ||
    value === 'invalid_response' ||
    value === 'error'
  );
}

function isDocumentReadyState(value: unknown): value is DocumentReadyState | null {
  return value === null || value === 'loading' || value === 'interactive' || value === 'complete';
}

function isFieldMilestones(value: unknown): value is ShikihoFieldMilestonesV1 {
  if (!isRecord(value)) return false;
  const expectedKeys = [
    'commentary',
    'comparisonCompanies',
    'consolidatedBusinesses',
    'coreReady',
    'editionLabel',
    'features',
    'identity',
    'industries',
    'marketThemes',
    'pageUpdatedAt',
    'profile',
    'quote',
    'score',
  ];
  return (
    hasExactKeys(value, expectedKeys) && expectedKeys.every((key) => isNullableBoundedNonnegativeNumber(value[key]))
  );
}

function isTraceNavigation(value: unknown): value is ShikihoCaptureTraceV1['navigation'] {
  return (
    isRecord(value) &&
    hasExactKeys(value, ['domContentLoadedMs', 'domInteractiveMs', 'loadEndMs', 'responseStartMs']) &&
    isNullableBoundedNonnegativeNumber(value.responseStartMs) &&
    isNullableBoundedNonnegativeNumber(value.domInteractiveMs) &&
    isNullableBoundedNonnegativeNumber(value.domContentLoadedMs) &&
    isNullableBoundedNonnegativeNumber(value.loadEndMs)
  );
}

function isTraceDom(value: unknown): value is ShikihoCaptureTraceV1['dom'] {
  if (
    !isRecord(value) ||
    !hasExactKeys(value, [
      'firstSampleMs',
      'firstSeenMs',
      'meaningfulChanges',
      'missingFields',
      'mutationBatches',
      'presentFields',
      'samples',
    ]) ||
    !isNullableBoundedNonnegativeNumber(value.firstSampleMs) ||
    !isNonnegativeSafeInteger(value.mutationBatches) ||
    !isNonnegativeSafeInteger(value.meaningfulChanges) ||
    !isNonnegativeSafeInteger(value.samples) ||
    !isUniqueTraceFieldArray(value.presentFields) ||
    !isUniqueTraceFieldArray(value.missingFields) ||
    !isFieldMilestones(value.firstSeenMs)
  ) {
    return false;
  }
  const present = new Set(value.presentFields);
  return value.missingFields.every((field) => !present.has(field));
}

function isTraceExtraction(value: unknown): value is ShikihoCaptureTraceV1['extraction'] {
  return (
    isRecord(value) &&
    hasExactKeys(value, ['lastMs', 'maxMs', 'samples', 'totalMs']) &&
    isNonnegativeSafeInteger(value.samples) &&
    isNullableBoundedNonnegativeNumber(value.lastMs) &&
    isNullableBoundedNonnegativeNumber(value.maxMs) &&
    isBoundedNonnegativeNumber(value.totalMs)
  );
}

function isTraceTimings(value: unknown): value is ShikihoCaptureTraceV1['timings'] {
  return (
    isRecord(value) &&
    hasExactKeys(value, ['acquisitionMs', 'domObservationMs', 'probeMs', 'receiverMs', 'storageMs', 'totalMs']) &&
    isBoundedNonnegativeNumber(value.probeMs) &&
    isBoundedNonnegativeNumber(value.acquisitionMs) &&
    isBoundedNonnegativeNumber(value.receiverMs) &&
    isBoundedNonnegativeNumber(value.domObservationMs) &&
    isBoundedNonnegativeNumber(value.storageMs) &&
    isBoundedNonnegativeNumber(value.totalMs)
  );
}

function hasCoherentTraceMode(trace: ShikihoCaptureTraceV1): boolean {
  const isUnbound = trace.mode === 'acquisition_unbound';
  const isAcquisitionPhase =
    trace.phase === 'queued' || trace.phase === 'probing_tabs' || trace.phase === 'acquiring_tab';
  if (!isUnbound) return !isAcquisitionPhase;
  const expectedReason = trace.outcome === 'timeout' ? 'deadline' : trace.outcome === 'error' ? 'error' : null;
  return (
    isAcquisitionPhase &&
    (trace.outcome === null || trace.outcome === 'timeout' || trace.outcome === 'error') &&
    trace.waitEndReason === expectedReason &&
    trace.receiverAttempts === 0 &&
    trace.receiverReadyMs === null &&
    trace.documentReadyState === null &&
    Object.values(trace.navigation).every((entry) => entry === null) &&
    trace.dom.firstSampleMs === null &&
    trace.dom.mutationBatches === 0 &&
    trace.dom.meaningfulChanges === 0 &&
    trace.dom.samples === 0 &&
    trace.dom.presentFields.length === 0 &&
    Object.values(trace.dom.firstSeenMs).every((entry) => entry === null) &&
    trace.extraction.samples === 0 &&
    trace.extraction.lastMs === null &&
    trace.extraction.maxMs === null &&
    trace.extraction.totalMs === 0 &&
    (trace.phase !== 'queued' || (trace.timings.probeMs === 0 && trace.timings.acquisitionMs === 0)) &&
    (trace.phase !== 'probing_tabs' || trace.timings.acquisitionMs === 0) &&
    trace.timings.receiverMs === 0 &&
    trace.timings.domObservationMs === 0 &&
    trace.timings.storageMs === 0
  );
}

export function parseShikihoCaptureTrace(value: unknown): ShikihoCaptureTraceV1 | null {
  if (
    !isRecord(value) ||
    !isWithinSnapshotLimit(value) ||
    !hasExactKeys(value, [
      'attemptId',
      'code',
      'documentReadyState',
      'dom',
      'extraction',
      'mode',
      'navigation',
      'outcome',
      'phase',
      'receiverAttempts',
      'receiverReadyMs',
      'schemaVersion',
      'startedAt',
      'timings',
      'updatedAt',
      'waitEndReason',
    ]) ||
    value.schemaVersion !== 1 ||
    !isLimitedString(value.attemptId, MAX_REQUEST_ID_LENGTH) ||
    !isExactCode(value.code) ||
    !isTraceMode(value.mode) ||
    !isTracePhase(value.phase) ||
    !isIsoTimestamp(value.startedAt) ||
    !isIsoTimestamp(value.updatedAt) ||
    !isTraceOutcome(value.outcome) ||
    !isWaitEndReason(value.waitEndReason) ||
    !isNonnegativeSafeInteger(value.receiverAttempts) ||
    !isNullableBoundedNonnegativeNumber(value.receiverReadyMs) ||
    !isDocumentReadyState(value.documentReadyState) ||
    !isTraceNavigation(value.navigation) ||
    !isTraceDom(value.dom) ||
    !isTraceExtraction(value.extraction) ||
    !isTraceTimings(value.timings)
  ) {
    return null;
  }
  const trace = value as unknown as ShikihoCaptureTraceV1;
  if (!hasCoherentTraceMode(trace)) return null;
  const totalMs = trace.timings.totalMs;
  const milestones = [trace.dom.firstSampleMs, ...Object.values(trace.dom.firstSeenMs)].filter(
    (milestone): milestone is number => milestone !== null
  );
  const phases = [
    trace.timings.probeMs,
    trace.timings.acquisitionMs,
    trace.timings.receiverMs,
    trace.timings.domObservationMs,
    trace.timings.storageMs,
  ];
  if (
    Date.parse(trace.updatedAt) < Date.parse(trace.startedAt) ||
    milestones.some((milestone) => milestone > totalMs) ||
    phases.some((phaseDuration) => phaseDuration > totalMs)
  ) {
    return null;
  }
  return trace;
}

export function parseShikihoCaptureProgress(value: unknown): ShikihoCaptureProgressV1 | null {
  if (
    !isRecord(value) ||
    !isWithinSnapshotLimit(value) ||
    !hasExactKeys(value, ['attemptId', 'candidate', 'code', 'schemaVersion', 'sequence', 'trace']) ||
    value.schemaVersion !== 1 ||
    !isLimitedString(value.attemptId, MAX_REQUEST_ID_LENGTH) ||
    !isExactCode(value.code) ||
    !isPositiveSafeInteger(value.sequence)
  ) {
    return null;
  }
  const candidate = value.candidate === null ? null : parseShikihoSnapshot(value.candidate);
  const trace = parseShikihoCaptureTrace(value.trace);
  if (
    (value.candidate !== null && candidate === null) ||
    trace === null ||
    trace.code !== value.code ||
    trace.attemptId !== value.attemptId ||
    (candidate !== null && candidate.code !== value.code)
  ) {
    return null;
  }
  return { ...value, candidate, trace } as ShikihoCaptureProgressV1;
}

function parseCaptureProgressBridgeResponse(value: UnknownRecord): ShikihoBridgeResponseV1 | null {
  if (
    !hasExactKeys(value, [
      'attemptId',
      'candidate',
      'channel',
      'code',
      'direction',
      'requestId',
      'sequence',
      'trace',
      'type',
    ])
  ) {
    return null;
  }
  const progress = parseShikihoCaptureProgress({
    schemaVersion: 1,
    attemptId: value.attemptId,
    code: value.code,
    sequence: value.sequence,
    candidate: value.candidate,
    trace: value.trace,
  });
  return progress === null ? null : (value as unknown as ShikihoBridgeResponseV1);
}

function parseSnapshotBridgeResponse(value: UnknownRecord): ShikihoBridgeResponseV1 | null {
  if (
    !isExactCode(value.code) ||
    !hasExactKeys(value, ['channel', 'code', 'diagnostic', 'direction', 'requestId', 'snapshot', 'trace', 'type'])
  ) {
    return null;
  }

  const snapshot = value.snapshot === null ? null : parseShikihoSnapshot(value.snapshot);
  const diagnostic = value.diagnostic === null ? null : parseShikihoDiagnostic(value.diagnostic);
  const trace = value.trace === null ? null : parseShikihoCaptureTrace(value.trace);
  if (
    (value.snapshot !== null && snapshot === null) ||
    (value.diagnostic !== null && diagnostic === null) ||
    (value.trace !== null && trace === null) ||
    (snapshot !== null && snapshot.code !== value.code) ||
    (diagnostic !== null && diagnostic.code !== value.code) ||
    (trace !== null && trace.code !== value.code)
  ) {
    return null;
  }
  return { ...value, snapshot, diagnostic, trace } as ShikihoBridgeResponseV1;
}

export function parseShikihoBridgeResponse(value: unknown): ShikihoBridgeResponseV1 | null {
  if (
    !isRecord(value) ||
    !isWithinSnapshotLimit(value) ||
    value.channel !== SHIKIHO_BRIDGE_CHANNEL ||
    value.direction !== 'extension-to-page' ||
    !isLimitedString(value.requestId, MAX_REQUEST_ID_LENGTH)
  ) {
    return null;
  }
  if (value.type === 'ready') {
    return hasExactKeys(value, ['channel', 'direction', 'requestId', 'type'])
      ? (value as ShikihoBridgeResponseV1)
      : null;
  }
  if (value.type === 'capture_progress') return parseCaptureProgressBridgeResponse(value);
  if (value.type === 'snapshot') return parseSnapshotBridgeResponse(value);
  return null;
}
