import {
  normalizeShikihoCode,
  parseShikihoBridgeResponse,
  SHIKIHO_BRIDGE_CHANNEL,
  type ShikihoBridgeRequestV1,
  type ShikihoBridgeResponseV1,
  type ShikihoCaptureDiagnosticV1,
  type ShikihoCaptureTraceV1,
  type ShikihoSnapshotV1,
} from '@trading25/shikiho-extension/contract';
import { useCallback, useEffect, useRef, useState } from 'react';

export type ShikihoBridgeStatus = 'checking' | 'available' | 'unavailable';

export type ShikihoCaptureState =
  | 'checking_extension'
  | 'extension_unavailable'
  | 'not_captured'
  | 'captured'
  | 'partial'
  | 'stale'
  | 'login_required'
  | 'page_changed'
  | 'storage_error';

export interface ShikihoSnapshotResult {
  bridgeStatus: ShikihoBridgeStatus;
  snapshot: ShikihoSnapshotV1 | null;
  displaySnapshot: ShikihoSnapshotV1 | null;
  candidate: ShikihoSnapshotV1 | null;
  trace: ShikihoCaptureTraceV1 | null;
  diagnostic: ShikihoCaptureDiagnosticV1 | null;
  captureState: ShikihoCaptureState;
  isRefreshing: boolean;
  refresh(): void;
}

type ShikihoSnapshotSelection = Omit<ShikihoSnapshotResult, 'isRefreshing' | 'refresh'>;

export interface ShikihoOwnedSnapshotState {
  ownerCode: string | null;
  snapshot: ShikihoSnapshotV1 | null;
  diagnostic: ShikihoCaptureDiagnosticV1 | null;
  trace?: ShikihoCaptureTraceV1 | null;
}

interface ShikihoActiveProgressState {
  ownerCode: string;
  requestId: string;
  attemptId: string;
  sequence: number;
  candidate: ShikihoSnapshotV1 | null;
  trace: ShikihoCaptureTraceV1;
}

type ShikihoProgressResponse = Extract<ShikihoBridgeResponseV1, { type: 'capture_progress' }>;
type ShikihoTerminalResponse = Extract<ShikihoBridgeResponseV1, { type: 'snapshot' }>;

const EXTENSION_AVAILABILITY_TIMEOUT_MS = 1_000;

function matchesCurrentCode(
  response: Extract<ShikihoBridgeResponseV1, { type: 'snapshot' }>,
  currentCode: string | null
): boolean {
  return (
    currentCode !== null &&
    response.code === currentCode &&
    (response.snapshot === null || response.snapshot.code === currentCode) &&
    (response.diagnostic === null || response.diagnostic.code === currentCode) &&
    (response.trace === null || response.trace.code === currentCode)
  );
}

function advanceShikihoProgress(
  response: ShikihoProgressResponse,
  currentCode: string | null,
  previous: ShikihoActiveProgressState | null,
  retiredAttemptIds: Set<string>
): ShikihoActiveProgressState | null {
  if (currentCode === null || response.code !== currentCode || response.trace.code !== currentCode) return null;
  if (retiredAttemptIds.has(response.attemptId)) return null;
  if (previous?.attemptId === response.attemptId && response.sequence <= previous.sequence) return null;
  if (previous !== null && previous.attemptId !== response.attemptId) {
    retiredAttemptIds.add(previous.attemptId);
  }
  const candidate =
    previous?.attemptId === response.attemptId
      ? mergeShikihoCandidateSnapshot(previous.candidate, response.candidate)
      : response.candidate;
  return {
    ownerCode: response.code,
    requestId: response.requestId,
    attemptId: response.attemptId,
    sequence: response.sequence,
    candidate,
    trace: response.trace,
  };
}

function mergeTerminalState(
  previous: ShikihoOwnedSnapshotState,
  response: ShikihoTerminalResponse
): ShikihoOwnedSnapshotState {
  const ownsCode = previous.ownerCode === response.code;
  return {
    ownerCode: response.code,
    snapshot: response.snapshot ?? (ownsCode ? previous.snapshot : null),
    diagnostic: response.diagnostic,
    trace: response.trace ?? (ownsCode ? (previous.trace ?? null) : null),
  };
}

const DISPLAY_FIELDS = [
  'features',
  'consolidatedBusinesses',
  'commentary',
  'score',
  'comparisonCompanies',
  'industries',
  'marketThemes',
  'profile',
  'editionLabel',
  'earningsAnnouncementDate',
  'pageUpdatedAt',
] as const;

function mergeShikihoCandidateSnapshot(
  previous: ShikihoSnapshotV1 | null,
  next: ShikihoSnapshotV1 | null
): ShikihoSnapshotV1 | null {
  if (next === null) return previous;
  if (previous === null || previous.code !== next.code) return next;

  const nextMissing = new Set(next.missingFields);
  const merged = { ...next };
  for (const field of DISPLAY_FIELDS) {
    if (nextMissing.has(field)) {
      Object.assign(merged, { [field]: previous[field] });
    }
  }
  merged.companyName = next.companyName ?? previous.companyName;
  merged.quote = next.quote ?? previous.quote;
  merged.missingFields = next.missingFields.filter((field) => previous.missingFields.includes(field));
  return merged;
}

export function mergeShikihoDisplaySnapshot(
  stable: ShikihoSnapshotV1 | null,
  candidate: ShikihoSnapshotV1 | null
): ShikihoSnapshotV1 | null {
  if (candidate === null) return stable;
  if (stable === null) return candidate;
  if (candidate.code !== stable.code) return stable;

  const candidateMissing = new Set(candidate.missingFields);
  const merged = { ...stable };
  for (const field of DISPLAY_FIELDS) {
    if (!candidateMissing.has(field)) {
      Object.assign(merged, { [field]: candidate[field] });
    }
  }
  if (candidate.companyName !== null) merged.companyName = candidate.companyName;
  merged.missingFields = candidate.missingFields.filter((field) => stable.missingFields.includes(field));
  return merged;
}

function captureStateFor(
  bridgeStatus: ShikihoBridgeStatus,
  snapshot: ShikihoSnapshotV1 | null,
  diagnostic: ShikihoCaptureDiagnosticV1 | null
): ShikihoCaptureState {
  if (bridgeStatus === 'checking') return 'checking_extension';
  if (bridgeStatus === 'unavailable') return 'extension_unavailable';
  if (snapshot !== null) {
    if (diagnostic !== null && Date.parse(diagnostic.observedAt) > Date.parse(snapshot.capturedAt)) return 'stale';
    return snapshot.status;
  }
  return diagnostic?.status ?? 'not_captured';
}

export function selectShikihoSnapshotState(
  currentCode: string | null,
  bridgeStatus: ShikihoBridgeStatus,
  ownedState: ShikihoOwnedSnapshotState
): ShikihoSnapshotSelection {
  if (ownedState.ownerCode !== currentCode) {
    return {
      bridgeStatus,
      snapshot: null,
      displaySnapshot: null,
      candidate: null,
      trace: null,
      diagnostic: null,
      captureState: bridgeStatus === 'unavailable' ? 'extension_unavailable' : 'checking_extension',
    };
  }

  return {
    bridgeStatus,
    snapshot: ownedState.snapshot,
    displaySnapshot: ownedState.snapshot,
    candidate: null,
    trace: ownedState.trace ?? null,
    diagnostic: ownedState.diagnostic,
    captureState: captureStateFor(bridgeStatus, ownedState.snapshot, ownedState.diagnostic),
  };
}

export function useShikihoSnapshot(symbol: string | null): ShikihoSnapshotResult {
  const currentCode = normalizeShikihoCode(symbol);
  const [bridgeStatus, setBridgeStatus] = useState<ShikihoBridgeStatus>('checking');
  const [ownedState, setOwnedState] = useState<ShikihoOwnedSnapshotState>({
    ownerCode: null,
    snapshot: null,
    diagnostic: null,
    trace: null,
  });
  const [activeProgress, setActiveProgress] = useState<ShikihoActiveProgressState | null>(null);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const currentCodeRef = useRef<string | null>(null);
  const currentRequestIdRef = useRef<string | null>(null);
  const availabilityTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const activeProgressRef = useRef<ShikihoActiveProgressState | null>(null);
  const retiredAttemptIdsRef = useRef(new Set<string>());
  const terminalRequestIdRef = useRef<string | null>(null);

  useEffect(() => {
    const markAvailable = (): void => {
      if (availabilityTimerRef.current !== null) {
        clearTimeout(availabilityTimerRef.current);
        availabilityTimerRef.current = null;
      }
      setBridgeStatus('available');
    };

    const acceptProgress = (response: ShikihoProgressResponse): void => {
      if (terminalRequestIdRef.current === response.requestId) return;
      const next = advanceShikihoProgress(
        response,
        currentCodeRef.current,
        activeProgressRef.current,
        retiredAttemptIdsRef.current
      );
      if (next === null) return;
      activeProgressRef.current = next;
      setActiveProgress(next);
      markAvailable();
    };

    const acceptTerminal = (response: ShikihoTerminalResponse): void => {
      if (!matchesCurrentCode(response, currentCodeRef.current)) return;
      const active = activeProgressRef.current;

      markAvailable();
      setOwnedState((previous) => mergeTerminalState(previous, response));
      if (active !== null) retiredAttemptIdsRef.current.add(active.attemptId);
      terminalRequestIdRef.current = response.requestId;
      activeProgressRef.current = null;
      setActiveProgress(null);
      setIsRefreshing(false);
    };

    const onMessage = (event: MessageEvent): void => {
      if (event.source !== window) return;
      const response = parseShikihoBridgeResponse(event.data);
      if (response === null || response.requestId !== currentRequestIdRef.current) return;

      if (response.type === 'ready') {
        markAvailable();
        return;
      }

      if (response.type === 'capture_progress') {
        acceptProgress(response);
        return;
      }
      acceptTerminal(response);
    };

    window.addEventListener('message', onMessage);
    return () => window.removeEventListener('message', onMessage);
  }, []);

  const dispatchRequest = useCallback((code: string, forceRefresh: boolean): void => {
    const requestId = crypto.randomUUID();
    currentRequestIdRef.current = requestId;
    terminalRequestIdRef.current = null;
    activeProgressRef.current = null;
    retiredAttemptIdsRef.current.clear();
    setActiveProgress(null);
    setIsRefreshing(true);
    const ping: ShikihoBridgeRequestV1 = {
      channel: SHIKIHO_BRIDGE_CHANNEL,
      direction: 'page-to-extension',
      type: 'ping',
      requestId,
    };
    const request: ShikihoBridgeRequestV1 = {
      channel: SHIKIHO_BRIDGE_CHANNEL,
      direction: 'page-to-extension',
      type: 'get_snapshot',
      requestId,
      code,
      forceRefresh,
    };

    window.postMessage(ping, window.location.origin);
    window.postMessage(request, window.location.origin);
    availabilityTimerRef.current = setTimeout(() => {
      if (currentRequestIdRef.current === requestId) {
        setBridgeStatus('unavailable');
        activeProgressRef.current = null;
        setActiveProgress(null);
        setIsRefreshing(false);
      }
    }, EXTENSION_AVAILABILITY_TIMEOUT_MS);
  }, []);

  useEffect(() => {
    const code = currentCode;
    currentCodeRef.current = code;
    terminalRequestIdRef.current = null;
    activeProgressRef.current = null;
    retiredAttemptIdsRef.current.clear();
    setActiveProgress(null);
    setBridgeStatus('checking');

    if (availabilityTimerRef.current !== null) clearTimeout(availabilityTimerRef.current);
    if (code === null) {
      currentRequestIdRef.current = null;
      setIsRefreshing(false);
      return;
    }
    dispatchRequest(code, false);

    return () => {
      if (availabilityTimerRef.current !== null) {
        clearTimeout(availabilityTimerRef.current);
        availabilityTimerRef.current = null;
      }
    };
  }, [currentCode, dispatchRequest]);

  const refresh = useCallback((): void => {
    const code = currentCodeRef.current;
    if (code === null) return;
    if (availabilityTimerRef.current !== null) clearTimeout(availabilityTimerRef.current);
    dispatchRequest(code, true);
  }, [dispatchRequest]);

  const selected = selectShikihoSnapshotState(currentCode, bridgeStatus, ownedState);
  const currentProgress = activeProgress?.ownerCode === currentCode ? activeProgress : null;
  return {
    ...selected,
    displaySnapshot: mergeShikihoDisplaySnapshot(selected.snapshot, currentProgress?.candidate ?? null),
    candidate: currentProgress?.candidate ?? null,
    trace: currentProgress?.trace ?? selected.trace,
    isRefreshing,
    refresh,
  };
}
