import {
  normalizeShikihoCode,
  parseShikihoBridgeResponse,
  SHIKIHO_BRIDGE_CHANNEL,
  type ShikihoBridgeRequestV1,
  type ShikihoBridgeResponseV1,
  type ShikihoCaptureDiagnosticV1,
  type ShikihoSnapshotV1,
} from '@trading25/shikiho-extension/contract';
import { useEffect, useRef, useState } from 'react';

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

export function useShikihoSnapshot(symbol: string | null): ShikihoSnapshotResult {
  const [bridgeStatus, setBridgeStatus] = useState<ShikihoBridgeStatus>('checking');
  const [snapshot, setSnapshot] = useState<ShikihoSnapshotV1 | null>(null);
  const [diagnostic, setDiagnostic] = useState<ShikihoCaptureDiagnosticV1 | null>(null);
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
      setSnapshot(response.snapshot);
      setDiagnostic(response.diagnostic);
    };

    window.addEventListener('message', onMessage);
    return () => window.removeEventListener('message', onMessage);
  }, []);

  useEffect(() => {
    const code = normalizeShikihoCode(symbol);
    currentCodeRef.current = code;
    setSnapshot(null);
    setDiagnostic(null);
    setBridgeStatus('checking');

    if (availabilityTimerRef.current !== null) clearTimeout(availabilityTimerRef.current);
    if (code === null) {
      currentRequestIdRef.current = null;
      return;
    }

    const requestId = crypto.randomUUID();
    currentRequestIdRef.current = requestId;
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
    };

    window.postMessage(ping, window.location.origin);
    window.postMessage(request, window.location.origin);
    availabilityTimerRef.current = setTimeout(() => {
      if (currentRequestIdRef.current === requestId) setBridgeStatus('unavailable');
    }, EXTENSION_AVAILABILITY_TIMEOUT_MS);

    return () => {
      if (availabilityTimerRef.current !== null) {
        clearTimeout(availabilityTimerRef.current);
        availabilityTimerRef.current = null;
      }
    };
  }, [symbol]);

  return {
    bridgeStatus,
    snapshot,
    diagnostic,
    captureState: captureStateFor(bridgeStatus, snapshot, diagnostic),
  };
}
