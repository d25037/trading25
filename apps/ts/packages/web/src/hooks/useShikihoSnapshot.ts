import {
  normalizeShikihoCode,
  parseShikihoBridgeResponse,
  SHIKIHO_BRIDGE_CHANNEL,
  type ShikihoBridgeRequestV1,
  type ShikihoBridgeResponseV1,
  type ShikihoCaptureDiagnosticV1,
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
}

const EXTENSION_AVAILABILITY_TIMEOUT_MS = 1_000;

function matchesCurrentCode(
  response: Extract<ShikihoBridgeResponseV1, { type: 'snapshot' }>,
  currentCode: string | null
): boolean {
  return (
    currentCode !== null &&
    response.code === currentCode &&
    (response.snapshot === null || response.snapshot.code === currentCode) &&
    (response.diagnostic === null || response.diagnostic.code === currentCode)
  );
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
      diagnostic: null,
      captureState: bridgeStatus === 'unavailable' ? 'extension_unavailable' : 'checking_extension',
    };
  }

  return {
    bridgeStatus,
    snapshot: ownedState.snapshot,
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
  });
  const [isRefreshing, setIsRefreshing] = useState(false);
  const currentCodeRef = useRef<string | null>(null);
  const currentRequestIdRef = useRef<string | null>(null);
  const availabilityTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    const markAvailable = (): void => {
      if (availabilityTimerRef.current !== null) {
        clearTimeout(availabilityTimerRef.current);
        availabilityTimerRef.current = null;
      }
      setBridgeStatus('available');
    };

    const onMessage = (event: MessageEvent): void => {
      if (event.source !== window) return;
      const response = parseShikihoBridgeResponse(event.data);
      if (response === null || response.requestId !== currentRequestIdRef.current) return;

      if (response.type === 'ready') {
        markAvailable();
        return;
      }

      if (!matchesCurrentCode(response, currentCodeRef.current)) return;

      markAvailable();
      setOwnedState({ ownerCode: response.code, snapshot: response.snapshot, diagnostic: response.diagnostic });
      setIsRefreshing(false);
    };

    window.addEventListener('message', onMessage);
    return () => window.removeEventListener('message', onMessage);
  }, []);

  const dispatchRequest = useCallback((code: string, forceRefresh: boolean): void => {
    const requestId = crypto.randomUUID();
    currentRequestIdRef.current = requestId;
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
      if (currentRequestIdRef.current === requestId) setBridgeStatus('unavailable');
    }, EXTENSION_AVAILABILITY_TIMEOUT_MS);
  }, []);

  useEffect(() => {
    const code = currentCode;
    currentCodeRef.current = code;
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

  return {
    ...selectShikihoSnapshotState(currentCode, bridgeStatus, ownedState),
    isRefreshing,
    refresh,
  };
}
