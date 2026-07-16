import { normalizeShikihoCode } from './contract';

export const SHIKIHO_WARM_TAB_IDLE_MS = 3 * 60 * 1000;
export const SHIKIHO_WARM_TAB_MAX_AGE_MS = 5 * 60 * 1000;
export const SHIKIHO_WARM_TAB_LEASE_KEY = 'shikihoWarmTabLeaseV1';

const ALARM_PREFIX = 'shikiho-warm-tab';

export type WarmTabMode = 'warm_owned_same_code' | 'warm_owned_navigation' | 'new_owned_tab';

export interface ShikihoWarmTabLeaseV1 {
  version: 1;
  tabId: number;
  ownerToken: string;
  generation: number;
  phase: 'capturing' | 'idle';
  code: string | null;
  createdAt: number;
  idleDeadline: number | null;
}

export interface WarmTabHandle {
  lease: ShikihoWarmTabLeaseV1;
  mode: WarmTabMode;
}

export interface WarmTabLeaseDeps {
  now(): number;
  createOwnerToken(): string;
  tabs: {
    create(properties: { active: false; url: string }): Promise<{ id?: number }>;
    update(tabId: number, properties: { active: false; url: string }): Promise<unknown>;
    remove(tabId: number): Promise<void>;
    get(tabId: number): Promise<unknown>;
  };
  session: {
    get(key: string): Promise<unknown>;
    set(key: string, value: unknown): Promise<void>;
    remove(key: string): Promise<void>;
  };
  alarms: {
    create(name: string, when: number): Promise<void>;
    clear(name: string): Promise<boolean>;
  };
  hasShikihoStockContentScript(tabId: number): Promise<boolean>;
}

export interface WarmTabLeaseManager {
  getValidOwnedTabId(): Promise<number | null>;
  reconcile(): Promise<void>;
  acquire(code: string): Promise<WarmTabHandle>;
  releaseSuccess(handle: WarmTabHandle, code: string): Promise<void>;
  releaseFailure(handle: WarmTabHandle): Promise<void>;
  onAlarm(name: string): Promise<void>;
  onActivated(tabId: number): Promise<void>;
  abandonOwnedTab(tabId: number): Promise<void>;
  abandonIfOwned(tabId: number): Promise<void>;
  onUpdatedComplete(tabId: number): Promise<void>;
  onRemoved(tabId: number): Promise<void>;
}

interface AlarmIdentity {
  tabId: number;
  ownerToken: string;
  generation: number;
  deadline: number;
}

function isCanonicalCode(value: unknown): value is string {
  return typeof value === 'string' && normalizeShikihoCode(value) === value;
}

function isLease(value: unknown): value is ShikihoWarmTabLeaseV1 {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return false;
  const lease = value as Record<string, unknown>;
  if (
    lease.version !== 1 ||
    !Number.isInteger(lease.tabId) ||
    (lease.tabId as number) < 0 ||
    typeof lease.ownerToken !== 'string' ||
    lease.ownerToken.length === 0 ||
    !Number.isInteger(lease.generation) ||
    (lease.generation as number) < 1 ||
    (lease.phase !== 'capturing' && lease.phase !== 'idle') ||
    (lease.code !== null && !isCanonicalCode(lease.code)) ||
    typeof lease.createdAt !== 'number' ||
    !Number.isFinite(lease.createdAt)
  ) {
    return false;
  }
  if (lease.phase === 'capturing') return lease.idleDeadline === null;
  return typeof lease.idleDeadline === 'number' && Number.isFinite(lease.idleDeadline);
}

function sameLease(left: ShikihoWarmTabLeaseV1, right: ShikihoWarmTabLeaseV1): boolean {
  return (
    left.version === right.version &&
    left.tabId === right.tabId &&
    left.ownerToken === right.ownerToken &&
    left.generation === right.generation &&
    left.phase === right.phase &&
    left.code === right.code &&
    left.createdAt === right.createdAt &&
    left.idleDeadline === right.idleDeadline
  );
}

function activeIdentity(lease: ShikihoWarmTabLeaseV1): string {
  return `${lease.ownerToken}\u0000${lease.generation}`;
}

function stockUrl(code: string): string {
  return `https://shikiho.toyokeizai.net/stocks/${code}`;
}

function alarmName(lease: ShikihoWarmTabLeaseV1): string | null {
  if (lease.phase !== 'idle' || lease.idleDeadline === null) return null;
  return [ALARM_PREFIX, lease.tabId, encodeURIComponent(lease.ownerToken), lease.generation, lease.idleDeadline].join(
    ':'
  );
}

function parseAlarmName(name: string): AlarmIdentity | null {
  const parts = name.split(':');
  if (parts.length !== 5 || parts[0] !== ALARM_PREFIX) return null;
  const tabId = Number(parts[1]);
  const generation = Number(parts[3]);
  const deadline = Number(parts[4]);
  let ownerToken: string;
  try {
    ownerToken = decodeURIComponent(parts[2] ?? '');
  } catch {
    return null;
  }
  if (
    !Number.isInteger(tabId) ||
    tabId < 0 ||
    ownerToken.length === 0 ||
    !Number.isInteger(generation) ||
    generation < 1 ||
    !Number.isFinite(deadline)
  ) {
    return null;
  }
  return { tabId, ownerToken, generation, deadline };
}

export function createWarmTabLeaseManager(deps: WarmTabLeaseDeps): WarmTabLeaseManager {
  const activeCaptures = new Map<string, number>();
  const provisionalOwners = new Map<number, string>();
  const adoptionEpochs = new Map<number, number>();
  const adoptedTabs = new Set<number>();
  let acquisitionTail: Promise<unknown> = Promise.resolve();

  function adoptionEpoch(tabId: number): number {
    return adoptionEpochs.get(tabId) ?? 0;
  }

  function advanceOwnershipEpoch(tabId: number): number {
    const next = adoptionEpoch(tabId) + 1;
    adoptionEpochs.set(tabId, next);
    provisionalOwners.delete(tabId);
    return next;
  }

  function markAdopted(tabId: number): number {
    const epoch = advanceOwnershipEpoch(tabId);
    adoptedTabs.add(tabId);
    return epoch;
  }

  function beginOwnedGeneration(tabId: number): number {
    const epoch = advanceOwnershipEpoch(tabId);
    adoptedTabs.delete(tabId);
    return epoch;
  }

  function forgetTab(tabId: number): number {
    const epoch = advanceOwnershipEpoch(tabId);
    adoptedTabs.delete(tabId);
    return epoch;
  }

  async function abandonPersistedOwnership(
    tabId: number,
    expectedEpoch: number,
    wasProvisional: boolean
  ): Promise<void> {
    const lease = await readLease();
    if (adoptionEpoch(tabId) !== expectedEpoch) return;
    if (lease?.tabId === tabId) {
      await abandonExact(lease);
      return;
    }
    if (!wasProvisional) adoptedTabs.delete(tabId);
  }

  function adoptAndAbandonOwnership(tabId: number): Promise<void> {
    const wasProvisional = provisionalOwners.has(tabId);
    return abandonPersistedOwnership(tabId, markAdopted(tabId), wasProvisional);
  }

  function forgetAndAbandonOwnership(tabId: number): Promise<void> {
    return abandonPersistedOwnership(tabId, forgetTab(tabId), false);
  }

  async function readLease(): Promise<ShikihoWarmTabLeaseV1 | null> {
    const value = await deps.session.get(SHIKIHO_WARM_TAB_LEASE_KEY);
    if (value === undefined) return null;
    if (isLease(value)) return value;
    await deps.session.remove(SHIKIHO_WARM_TAB_LEASE_KEY);
    return null;
  }

  async function removeMetadataIfCurrent(expected: ShikihoWarmTabLeaseV1): Promise<boolean> {
    const current = await readLease();
    if (current === null || !sameLease(current, expected)) return false;
    await deps.session.remove(SHIKIHO_WARM_TAB_LEASE_KEY);
    activeCaptures.delete(activeIdentity(expected));
    return true;
  }

  async function clearAlarmFor(lease: ShikihoWarmTabLeaseV1): Promise<void> {
    const name = alarmName(lease);
    if (name !== null) await deps.alarms.clear(name).catch(() => false);
  }

  async function abandonExact(expected: ShikihoWarmTabLeaseV1): Promise<void> {
    await clearAlarmFor(expected);
    await removeMetadataIfCurrent(expected);
  }

  async function closeExact(expected: ShikihoWarmTabLeaseV1, expectedEpoch: number): Promise<void> {
    await clearAlarmFor(expected);
    const current = await readLease();
    if (current === null || !sameLease(current, expected)) return;
    if (adoptionEpoch(expected.tabId) !== expectedEpoch || adoptedTabs.has(expected.tabId)) return;
    try {
      await deps.tabs.remove(expected.tabId);
    } catch {
      // The user may already have closed the tab. Exact metadata still belongs to this cleanup.
    }
    await removeMetadataIfCurrent(expected);
  }

  async function reconcile(): Promise<void> {
    const expectedEpochs = new Map(adoptionEpochs);
    const lease = await readLease();
    if (lease === null) return;
    const expectedEpoch = expectedEpochs.get(lease.tabId) ?? 0;
    try {
      await deps.tabs.get(lease.tabId);
    } catch {
      await removeMetadataIfCurrent(lease);
      return;
    }

    if (lease.phase === 'capturing') {
      if (!activeCaptures.has(activeIdentity(lease))) await closeExact(lease, expectedEpoch);
      return;
    }

    await closeExact(lease, expectedEpoch);
  }

  async function getValidOwnedTabId(): Promise<number | null> {
    return (await readLease())?.tabId ?? null;
  }

  async function createOwnedTab(code: string): Promise<WarmTabHandle> {
    const tab = await deps.tabs.create({ active: false, url: stockUrl(code) });
    if (tab.id === undefined) throw new Error('Created Shikiho tab has no id');
    const ownerToken = deps.createOwnerToken();
    const expectedEpoch = beginOwnedGeneration(tab.id);
    provisionalOwners.set(tab.id, ownerToken);
    const lease: ShikihoWarmTabLeaseV1 = {
      version: 1,
      tabId: tab.id,
      ownerToken,
      generation: 1,
      phase: 'capturing',
      code,
      createdAt: deps.now(),
      idleDeadline: null,
    };
    try {
      await deps.session.set(SHIKIHO_WARM_TAB_LEASE_KEY, lease);
    } catch (error) {
      if (provisionalOwners.get(tab.id) === ownerToken && adoptionEpoch(tab.id) === expectedEpoch) {
        provisionalOwners.delete(tab.id);
        await deps.tabs.remove(tab.id).catch(() => undefined);
        await removeMetadataIfCurrent(lease).catch(() => false);
      }
      throw error;
    }
    if (provisionalOwners.get(tab.id) !== ownerToken || adoptionEpoch(tab.id) !== expectedEpoch) {
      await removeMetadataIfCurrent(lease);
      throw new Error('Warm-tab ownership was abandoned during creation');
    }
    provisionalOwners.delete(tab.id);
    activeCaptures.set(activeIdentity(lease), expectedEpoch);
    return { lease, mode: 'new_owned_tab' };
  }

  async function acquireSerialized(code: string): Promise<WarmTabHandle> {
    if (!isCanonicalCode(code)) throw new Error(`Expected a canonical four-character Shikiho code: ${code}`);
    await reconcile();
    const current = await readLease();
    if (current?.phase === 'capturing') throw new Error('A warm-tab capture is already active');
    return createOwnedTab(code);
  }

  function acquire(code: string): Promise<WarmTabHandle> {
    const result = acquisitionTail.then(() => acquireSerialized(code));
    acquisitionTail = result.then(
      () => undefined,
      () => undefined
    );
    return result;
  }

  async function releaseSuccess(handle: WarmTabHandle, code: string): Promise<void> {
    if (!isCanonicalCode(code)) throw new Error(`Expected a canonical four-character Shikiho code: ${code}`);
    const identity = activeIdentity(handle.lease);
    const expectedEpoch = activeCaptures.get(identity);
    activeCaptures.delete(identity);
    if (expectedEpoch === undefined) return;
    await closeExact(handle.lease, expectedEpoch);
  }

  async function releaseFailure(handle: WarmTabHandle): Promise<void> {
    const identity = activeIdentity(handle.lease);
    const expectedEpoch = activeCaptures.get(identity);
    activeCaptures.delete(identity);
    if (expectedEpoch === undefined) return;
    await closeExact(handle.lease, expectedEpoch);
  }

  async function onAlarm(name: string): Promise<void> {
    const identity = parseAlarmName(name);
    if (identity === null) return;
    const expectedEpoch = adoptionEpoch(identity.tabId);
    const lease = await readLease();
    if (
      lease === null ||
      lease.phase !== 'idle' ||
      lease.tabId !== identity.tabId ||
      lease.ownerToken !== identity.ownerToken ||
      lease.generation !== identity.generation ||
      lease.idleDeadline !== identity.deadline ||
      deps.now() < identity.deadline
    ) {
      return;
    }
    await closeExact(lease, expectedEpoch);
  }

  async function onActivated(tabId: number): Promise<void> {
    await adoptAndAbandonOwnership(tabId);
  }

  async function abandonOwnedTab(tabId: number): Promise<void> {
    await adoptAndAbandonOwnership(tabId);
  }

  async function abandonIfOwned(tabId: number): Promise<void> {
    const hasProvisionalOwner = provisionalOwners.has(tabId);
    const lease = await readLease();
    if (!hasProvisionalOwner && lease?.tabId !== tabId) return;
    const stillHosted = await deps.hasShikihoStockContentScript(tabId).catch(() => false);
    if (stillHosted) return;
    await adoptAndAbandonOwnership(tabId);
  }

  async function onUpdatedComplete(tabId: number): Promise<void> {
    const observed = await readLease();
    if (observed?.tabId !== tabId || observed.phase === 'capturing') return;
    const stillHosted = await deps.hasShikihoStockContentScript(tabId).catch(() => false);
    if (stillHosted) return;
    const current = await readLease();
    if (current?.phase !== 'idle' || !sameLease(current, observed)) return;
    await abandonExact(observed);
  }

  async function onRemoved(tabId: number): Promise<void> {
    await forgetAndAbandonOwnership(tabId);
  }

  return {
    getValidOwnedTabId,
    reconcile,
    acquire,
    releaseSuccess,
    releaseFailure,
    onAlarm,
    onActivated,
    abandonOwnedTab,
    abandonIfOwned,
    onUpdatedComplete,
    onRemoved,
  };
}
