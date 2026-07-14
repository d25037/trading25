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
  reconcile(): Promise<void>;
  acquire(code: string): Promise<WarmTabHandle>;
  releaseSuccess(handle: WarmTabHandle, code: string): Promise<void>;
  releaseFailure(handle: WarmTabHandle): Promise<void>;
  onAlarm(name: string): Promise<void>;
  onActivated(tabId: number): Promise<void>;
  abandonIfOwned(tabId: number): Promise<void>;
  onRemoved(tabId: number): Promise<void>;
}

interface AlarmIdentity {
  tabId: number;
  ownerToken: string;
  generation: number;
  deadline: number;
}

function isCanonicalCode(value: unknown): value is string {
  return typeof value === 'string' && /^\d{4}$/.test(value);
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
  const activeCaptures = new Set<string>();

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
    if (name !== null) await deps.alarms.clear(name);
  }

  async function abandonExact(expected: ShikihoWarmTabLeaseV1): Promise<void> {
    await clearAlarmFor(expected);
    await removeMetadataIfCurrent(expected);
  }

  async function closeExact(expected: ShikihoWarmTabLeaseV1): Promise<void> {
    await clearAlarmFor(expected);
    const current = await readLease();
    if (current === null || !sameLease(current, expected)) return;
    try {
      await deps.tabs.remove(expected.tabId);
    } catch {
      // The user may already have closed the tab. Exact metadata still belongs to this cleanup.
    }
    await removeMetadataIfCurrent(expected);
  }

  async function reconcile(): Promise<void> {
    const lease = await readLease();
    if (lease === null) return;
    try {
      await deps.tabs.get(lease.tabId);
    } catch {
      await removeMetadataIfCurrent(lease);
      return;
    }

    if (lease.phase === 'capturing') {
      if (!activeCaptures.has(activeIdentity(lease))) await closeExact(lease);
      return;
    }

    if (lease.idleDeadline === null) return;
    const deadline = Math.min(lease.idleDeadline, lease.createdAt + SHIKIHO_WARM_TAB_MAX_AGE_MS);
    if (deps.now() >= deadline) {
      await closeExact(lease);
      return;
    }
    await deps.alarms.create(alarmName(lease) as string, deadline);
  }

  async function createOwnedTab(code: string): Promise<WarmTabHandle> {
    const tab = await deps.tabs.create({ active: false, url: stockUrl(code) });
    if (tab.id === undefined) throw new Error('Created Shikiho tab has no id');
    const lease: ShikihoWarmTabLeaseV1 = {
      version: 1,
      tabId: tab.id,
      ownerToken: deps.createOwnerToken(),
      generation: 1,
      phase: 'capturing',
      code,
      createdAt: deps.now(),
      idleDeadline: null,
    };
    await deps.session.set(SHIKIHO_WARM_TAB_LEASE_KEY, lease);
    activeCaptures.add(activeIdentity(lease));
    return { lease, mode: 'new_owned_tab' };
  }

  async function navigateReusableTab(lease: ShikihoWarmTabLeaseV1, code: string): Promise<void> {
    const current = await readLease();
    if (current === null || !sameLease(current, lease)) {
      activeCaptures.delete(activeIdentity(lease));
      throw new Error('Warm-tab ownership changed before navigation');
    }
    try {
      await deps.tabs.update(lease.tabId, { active: false, url: stockUrl(code) });
    } catch (error) {
      activeCaptures.delete(activeIdentity(lease));
      await closeExact(lease);
      throw error;
    }
  }

  async function reuseOwnedTab(reusable: ShikihoWarmTabLeaseV1, code: string): Promise<WarmTabHandle> {
    await clearAlarmFor(reusable);
    const stillReusable = await readLease();
    if (stillReusable === null || !sameLease(stillReusable, reusable)) {
      throw new Error('Warm-tab ownership changed during acquisition');
    }
    const next: ShikihoWarmTabLeaseV1 = {
      ...reusable,
      generation: reusable.generation + 1,
      phase: 'capturing',
      code,
      idleDeadline: null,
    };
    await deps.session.set(SHIKIHO_WARM_TAB_LEASE_KEY, next);
    activeCaptures.add(activeIdentity(next));
    const mode: WarmTabMode = reusable.code === code ? 'warm_owned_same_code' : 'warm_owned_navigation';
    if (mode === 'warm_owned_navigation') await navigateReusableTab(next, code);
    return { lease: next, mode };
  }

  async function acquire(code: string): Promise<WarmTabHandle> {
    if (!isCanonicalCode(code)) throw new Error(`Expected a canonical four-digit Shikiho code: ${code}`);
    await reconcile();
    const reusable = await readLease();
    if (reusable?.phase === 'capturing') throw new Error('A warm-tab capture is already active');
    return reusable?.phase === 'idle' ? reuseOwnedTab(reusable, code) : createOwnedTab(code);
  }

  async function releaseSuccess(handle: WarmTabHandle, code: string): Promise<void> {
    if (!isCanonicalCode(code)) throw new Error(`Expected a canonical four-digit Shikiho code: ${code}`);
    activeCaptures.delete(activeIdentity(handle.lease));
    const current = await readLease();
    if (current === null || !sameLease(current, handle.lease) || current.phase !== 'capturing') return;
    if (deps.now() >= current.createdAt + SHIKIHO_WARM_TAB_MAX_AGE_MS) {
      await closeExact(current);
      return;
    }

    const idleDeadline = Math.min(
      deps.now() + SHIKIHO_WARM_TAB_IDLE_MS,
      current.createdAt + SHIKIHO_WARM_TAB_MAX_AGE_MS
    );
    const idle: ShikihoWarmTabLeaseV1 = { ...current, phase: 'idle', code, idleDeadline };
    await deps.session.set(SHIKIHO_WARM_TAB_LEASE_KEY, idle);
    await deps.alarms.create(alarmName(idle) as string, idleDeadline);
  }

  async function releaseFailure(handle: WarmTabHandle): Promise<void> {
    activeCaptures.delete(activeIdentity(handle.lease));
    await closeExact(handle.lease);
  }

  async function onAlarm(name: string): Promise<void> {
    const identity = parseAlarmName(name);
    if (identity === null) return;
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
    await closeExact(lease);
  }

  async function onActivated(tabId: number): Promise<void> {
    const lease = await readLease();
    if (lease?.tabId === tabId) await abandonExact(lease);
  }

  async function abandonIfOwned(tabId: number): Promise<void> {
    const lease = await readLease();
    if (lease?.tabId !== tabId) return;
    const stillHosted = await deps.hasShikihoStockContentScript(tabId).catch(() => false);
    if (stillHosted) return;
    const current = await readLease();
    if (current !== null && sameLease(current, lease)) await abandonExact(current);
  }

  async function onRemoved(tabId: number): Promise<void> {
    const lease = await readLease();
    if (lease?.tabId === tabId) await abandonExact(lease);
  }

  return { reconcile, acquire, releaseSuccess, releaseFailure, onAlarm, onActivated, abandonIfOwned, onRemoved };
}
