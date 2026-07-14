import { describe, expect, test } from 'bun:test';
import {
  createWarmTabLeaseManager,
  SHIKIHO_WARM_TAB_IDLE_MS,
  SHIKIHO_WARM_TAB_LEASE_KEY,
  SHIKIHO_WARM_TAB_MAX_AGE_MS,
  type ShikihoWarmTabLeaseV1,
  type WarmTabLeaseDeps,
} from './warm-tab-lease';

const NOW = Date.parse('2026-07-14T03:00:00.000Z');

function url(code: string): string {
  return `https://shikiho.toyokeizai.net/stocks/${code}`;
}

function deferred() {
  let resolve!: () => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<void>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function harness(sharedSession = new Map<string, unknown>()) {
  let now = NOW;
  let nextTabId = 100;
  let nextToken = 0;
  const tabs = new Set<number>();
  const creates: Array<{ active: false; url: string }> = [];
  const updates: Array<{ tabId: number; properties: { active: false; url: string } }> = [];
  const removes: number[] = [];
  const gets: number[] = [];
  const alarms = new Map<string, number>();
  const alarmCreates: Array<{ name: string; when: number }> = [];
  const alarmClears: string[] = [];
  const probeResults = new Map<number, boolean>();
  const getFailures = new Set<number>();
  const removeFailures = new Set<number>();
  let failNextSessionSet = false;
  let failNextAlarmCreate = false;
  let pendingSessionSet: ReturnType<typeof deferred> | null = null;
  let sessionSetStarted: ReturnType<typeof deferred> | null = null;

  const deps: WarmTabLeaseDeps = {
    now: () => now,
    createOwnerToken: () => `owner-${++nextToken}`,
    tabs: {
      create: async (properties) => {
        creates.push(properties);
        const id = nextTabId++;
        tabs.add(id);
        return { id };
      },
      update: async (tabId, properties) => {
        updates.push({ tabId, properties });
        if (!tabs.has(tabId)) throw new Error('missing tab');
      },
      remove: async (tabId) => {
        removes.push(tabId);
        if (removeFailures.has(tabId)) throw new Error('already removed');
        tabs.delete(tabId);
      },
      get: async (tabId) => {
        gets.push(tabId);
        if (getFailures.has(tabId) || !tabs.has(tabId)) throw new Error('missing tab');
        return { id: tabId };
      },
    },
    session: {
      get: async (key) => sharedSession.get(key),
      set: async (key, value) => {
        if (pendingSessionSet !== null) {
          const pending = pendingSessionSet;
          pendingSessionSet = null;
          sessionSetStarted?.resolve();
          sessionSetStarted = null;
          await pending.promise;
        }
        if (failNextSessionSet) {
          failNextSessionSet = false;
          throw new Error('session set failed');
        }
        sharedSession.set(key, structuredClone(value));
      },
      remove: async (key) => {
        sharedSession.delete(key);
      },
    },
    alarms: {
      create: async (name, when) => {
        if (failNextAlarmCreate) {
          failNextAlarmCreate = false;
          throw new Error('alarm create failed');
        }
        alarmCreates.push({ name, when });
        alarms.set(name, when);
      },
      clear: async (name) => {
        alarmClears.push(name);
        return alarms.delete(name);
      },
    },
    hasShikihoStockContentScript: async (tabId) => probeResults.get(tabId) ?? false,
  };

  return {
    deps,
    session: sharedSession,
    tabs,
    creates,
    updates,
    removes,
    gets,
    alarms,
    alarmCreates,
    alarmClears,
    probeResults,
    getFailures,
    removeFailures,
    manager: createWarmTabLeaseManager(deps),
    setNow(value: number) {
      now = value;
    },
    failSessionSet() {
      failNextSessionSet = true;
    },
    failAlarmCreate() {
      failNextAlarmCreate = true;
    },
    deferSessionSet() {
      const pending = deferred();
      const started = deferred();
      pendingSessionSet = pending;
      sessionSetStarted = started;
      return { ...pending, started: started.promise };
    },
  };
}

function storedLease(session: Map<string, unknown>): ShikihoWarmTabLeaseV1 | undefined {
  return session.get(SHIKIHO_WARM_TAB_LEASE_KEY) as ShikihoWarmTabLeaseV1 | undefined;
}

describe('warm tab acquisition and reuse', () => {
  test('creates and records the first owned tab as a capturing lease', async () => {
    const h = harness();

    const handle = await h.manager.acquire('7203');

    expect(h.creates).toEqual([{ active: false, url: url('7203') }]);
    expect(handle.mode).toBe('new_owned_tab');
    expect(handle.lease).toEqual({
      version: 1,
      tabId: 100,
      ownerToken: 'owner-1',
      generation: 1,
      phase: 'capturing',
      code: '7203',
      createdAt: NOW,
      idleDeadline: null,
    });
    expect(storedLease(h.session)).toEqual(handle.lease);
  });

  test('reuses an idle same-code tab without creating or navigating', async () => {
    const h = harness();
    const first = await h.manager.acquire('7203');
    await h.manager.releaseSuccess(first, '7203');

    const reused = await h.manager.acquire('7203');

    expect(reused.mode).toBe('warm_owned_same_code');
    expect(reused.lease.generation).toBe(2);
    expect(h.creates).toHaveLength(1);
    expect(h.updates).toHaveLength(0);
  });

  test('navigates an idle different-code tab after persisting its new generation', async () => {
    const h = harness();
    const first = await h.manager.acquire('7203');
    await h.manager.releaseSuccess(first, '7203');

    const reused = await h.manager.acquire('6758');

    expect(reused.mode).toBe('warm_owned_navigation');
    expect(reused.lease.generation).toBe(2);
    expect(h.updates).toEqual([{ tabId: 100, properties: { active: false, url: url('6758') } }]);
    expect(storedLease(h.session)).toEqual(reused.lease);
  });

  test('serializes concurrent acquisition so only one owned tab is created', async () => {
    const h = harness();

    const results = await Promise.allSettled([h.manager.acquire('7203'), h.manager.acquire('6758')]);

    expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(1);
    expect(results.filter((result) => result.status === 'rejected')).toHaveLength(1);
    expect(h.creates).toHaveLength(1);
    expect(h.tabs.size).toBe(1);
  });

  test('rolls back a newly created tab when session persistence fails', async () => {
    const h = harness();
    h.failSessionSet();

    await expect(h.manager.acquire('7203')).rejects.toThrow('session set failed');

    expect(h.creates).toHaveLength(1);
    expect(h.removes).toEqual([100]);
    expect(h.tabs.size).toBe(0);
    expect(storedLease(h.session)).toBeUndefined();
  });

  test('activation during successful create persistence abandons provisional ownership', async () => {
    const h = harness();
    const pending = h.deferSessionSet();
    const acquiring = h.manager.acquire('7203');
    await pending.started;

    await h.manager.onActivated(100);
    pending.resolve();

    await expect(acquiring).rejects.toThrow('ownership was abandoned');
    expect(h.removes).toHaveLength(0);
    expect(h.updates).toHaveLength(0);
    expect(h.tabs.has(100)).toBe(true);
    expect(storedLease(h.session)).toBeUndefined();
  });

  test('activation during failed create persistence prevents rollback close', async () => {
    const h = harness();
    const pending = h.deferSessionSet();
    const acquiring = h.manager.acquire('7203');
    await pending.started;

    await h.manager.onActivated(100);
    pending.reject(new Error('session set failed'));

    await expect(acquiring).rejects.toThrow('session set failed');
    expect(h.removes).toHaveLength(0);
    expect(h.updates).toHaveLength(0);
    expect(h.tabs.has(100)).toBe(true);
    expect(storedLease(h.session)).toBeUndefined();
  });

  test('restores the idle cleanup alarm when reuse persistence fails', async () => {
    const h = harness();
    const first = await h.manager.acquire('7203');
    await h.manager.releaseSuccess(first, '7203');
    const idle = storedLease(h.session);
    const alarm = h.alarmCreates[0];
    if (idle === undefined || alarm === undefined) throw new Error('expected idle lease and alarm');
    h.failSessionSet();

    await expect(h.manager.acquire('6758')).rejects.toThrow('session set failed');

    expect(storedLease(h.session)).toEqual(idle);
    expect(h.alarms.get(alarm.name)).toBe(alarm.when);
    expect(h.removes).toHaveLength(0);
    expect(h.updates).toHaveLength(0);
  });
});

describe('cleanup and ownership boundaries', () => {
  test('success or partial capture leaves one idle cleanup alarm', async () => {
    for (const code of ['7203', '6758']) {
      const h = harness();
      const handle = await h.manager.acquire(code);
      await h.manager.releaseSuccess(handle, code);

      expect(storedLease(h.session)).toMatchObject({
        phase: 'idle',
        code,
        idleDeadline: NOW + SHIKIHO_WARM_TAB_IDLE_MS,
      });
      expect(h.alarmCreates).toHaveLength(1);
      expect(h.alarmCreates[0]?.when).toBe(NOW + SHIKIHO_WARM_TAB_IDLE_MS);
    }
  });

  test('all terminal capture failures close and clear the exact owned lease', async () => {
    for (const _failure of ['login_required', 'page_changed', 'timeout', 'storage', 'invalid_response']) {
      const h = harness();
      const handle = await h.manager.acquire('7203');
      await h.manager.releaseFailure(handle);

      expect(h.removes).toEqual([100]);
      expect(storedLease(h.session)).toBeUndefined();
    }
  });

  test('an idle alarm closes only an exactly matching due lease', async () => {
    const h = harness();
    const handle = await h.manager.acquire('7203');
    await h.manager.releaseSuccess(handle, '7203');
    const alarmName = h.alarmCreates[0]?.name;
    if (alarmName === undefined) throw new Error('expected alarm');

    await h.manager.onAlarm(`${alarmName}-wrong`);
    expect(h.removes).toHaveLength(0);
    h.setNow(NOW + SHIKIHO_WARM_TAB_IDLE_MS);
    await h.manager.onAlarm(alarmName);

    expect(h.removes).toEqual([100]);
    expect(storedLease(h.session)).toBeUndefined();
  });

  test('an alarm with a mismatched tab ID closes nothing', async () => {
    const h = harness();
    const handle = await h.manager.acquire('7203');
    await h.manager.releaseSuccess(handle, '7203');
    const lease = storedLease(h.session);
    if (lease === undefined) throw new Error('expected lease');
    h.session.set(SHIKIHO_WARM_TAB_LEASE_KEY, { ...lease, tabId: 101 });
    h.setNow(lease.idleDeadline ?? NOW);

    await h.manager.onAlarm(h.alarmCreates[0]?.name ?? 'missing');

    expect(h.removes).toHaveLength(0);
  });

  test('an alarm with a mismatched owner token closes nothing', async () => {
    const h = harness();
    const handle = await h.manager.acquire('7203');
    await h.manager.releaseSuccess(handle, '7203');
    const lease = storedLease(h.session);
    if (lease === undefined) throw new Error('expected lease');
    h.session.set(SHIKIHO_WARM_TAB_LEASE_KEY, { ...lease, ownerToken: 'replacement-owner' });
    h.setNow(lease.idleDeadline ?? NOW);

    await h.manager.onAlarm(h.alarmCreates[0]?.name ?? 'missing');

    expect(h.removes).toHaveLength(0);
  });

  test('an alarm with a mismatched phase closes nothing', async () => {
    const h = harness();
    const handle = await h.manager.acquire('7203');
    await h.manager.releaseSuccess(handle, '7203');
    const lease = storedLease(h.session);
    if (lease === undefined) throw new Error('expected lease');
    h.session.set(SHIKIHO_WARM_TAB_LEASE_KEY, { ...lease, phase: 'capturing', idleDeadline: null });
    h.setNow(lease.idleDeadline ?? NOW);

    await h.manager.onAlarm(h.alarmCreates[0]?.name ?? 'missing');

    expect(h.removes).toHaveLength(0);
  });

  test('an alarm with a mismatched deadline closes nothing', async () => {
    const h = harness();
    const handle = await h.manager.acquire('7203');
    await h.manager.releaseSuccess(handle, '7203');
    const lease = storedLease(h.session);
    if (lease === undefined || lease.idleDeadline === null) throw new Error('expected idle lease');
    h.session.set(SHIKIHO_WARM_TAB_LEASE_KEY, { ...lease, idleDeadline: lease.idleDeadline + 1 });
    h.setNow(lease.idleDeadline + 1);

    await h.manager.onAlarm(h.alarmCreates[0]?.name ?? 'missing');

    expect(h.removes).toHaveLength(0);
  });

  test('an old alarm after reuse cannot close the new generation', async () => {
    const h = harness();
    const first = await h.manager.acquire('7203');
    await h.manager.releaseSuccess(first, '7203');
    const oldAlarm = h.alarmCreates[0]?.name;
    if (oldAlarm === undefined) throw new Error('expected alarm');
    await h.manager.acquire('6758');
    h.setNow(NOW + SHIKIHO_WARM_TAB_IDLE_MS);

    await h.manager.onAlarm(oldAlarm);

    expect(h.removes).toHaveLength(0);
    expect(storedLease(h.session)).toMatchObject({ generation: 2, phase: 'capturing', code: '6758' });
  });

  test('maximum age reached during capture closes immediately after success', async () => {
    const h = harness();
    const handle = await h.manager.acquire('7203');
    h.setNow(NOW + SHIKIHO_WARM_TAB_MAX_AGE_MS);

    await h.manager.releaseSuccess(handle, '7203');

    expect(h.removes).toEqual([100]);
    expect(storedLease(h.session)).toBeUndefined();
    expect(h.alarmCreates).toHaveLength(0);
  });

  test('alarm creation failure after success closes and clears the idle lease', async () => {
    const h = harness();
    const handle = await h.manager.acquire('7203');
    h.failAlarmCreate();

    await expect(h.manager.releaseSuccess(handle, '7203')).rejects.toThrow('alarm create failed');

    expect(h.removes).toEqual([100]);
    expect(storedLease(h.session)).toBeUndefined();
  });

  test('idle persistence failure after success closes only the exact capturing lease', async () => {
    const h = harness();
    const handle = await h.manager.acquire('7203');
    h.failSessionSet();

    await expect(h.manager.releaseSuccess(handle, '7203')).rejects.toThrow('session set failed');

    expect(h.removes).toEqual([100]);
    expect(storedLease(h.session)).toBeUndefined();
  });

  test('activation abandons ownership and never closes the user-adopted tab', async () => {
    const h = harness();
    const handle = await h.manager.acquire('7203');
    await h.manager.releaseSuccess(handle, '7203');

    await h.manager.onActivated(100);

    expect(h.removes).toHaveLength(0);
    expect(storedLease(h.session)).toBeUndefined();
    expect(h.alarmClears).toHaveLength(1);
  });

  test('failed owned-tab content-script probe abandons ownership without closing', async () => {
    const h = harness();
    const handle = await h.manager.acquire('7203');
    await h.manager.releaseSuccess(handle, '7203');
    h.probeResults.set(100, false);

    await h.manager.abandonIfOwned(100);

    expect(h.removes).toHaveLength(0);
    expect(storedLease(h.session)).toBeUndefined();
    expect(h.alarmClears).toHaveLength(1);
  });

  test('successful owned-tab content-script probe preserves ownership', async () => {
    const h = harness();
    const handle = await h.manager.acquire('7203');
    await h.manager.releaseSuccess(handle, '7203');
    h.probeResults.set(100, true);

    await h.manager.abandonIfOwned(100);

    expect(storedLease(h.session)).toMatchObject({ tabId: 100, phase: 'idle' });
    expect(h.alarmClears).toHaveLength(0);
  });

  test('tab removal clears matching metadata', async () => {
    const h = harness();
    await h.manager.acquire('7203');

    await h.manager.onRemoved(100);

    expect(storedLease(h.session)).toBeUndefined();
    expect(h.removes).toHaveLength(0);
  });

  test('missing or malformed session records never close a tab', async () => {
    for (const value of [undefined, null, {}, { version: 1, tabId: 100 }]) {
      const h = harness();
      h.tabs.add(100);
      if (value !== undefined) h.session.set(SHIKIHO_WARM_TAB_LEASE_KEY, value);

      await h.manager.reconcile();
      await h.manager.onAlarm('shikiho-warm-tab:100:owner-1:1:123');
      await h.manager.onActivated(100);

      expect(h.removes).toHaveLength(0);
    }
  });

  test('cleanup removes exact metadata even when tab removal rejects', async () => {
    const h = harness();
    const handle = await h.manager.acquire('7203');
    h.removeFailures.add(100);

    await h.manager.releaseFailure(handle);

    expect(h.removes).toEqual([100]);
    expect(storedLease(h.session)).toBeUndefined();
  });

  test('ownership replacement before failure prevents closing or clearing the replacement', async () => {
    const h = harness();
    const handle = await h.manager.acquire('7203');
    const replacement = { ...handle.lease, ownerToken: 'other-owner' };
    h.session.set(SHIKIHO_WARM_TAB_LEASE_KEY, replacement);

    await h.manager.releaseFailure(handle);

    expect(h.removes).toHaveLength(0);
    expect(storedLease(h.session)).toEqual(replacement);
  });
});

describe('manifest v3 reconciliation', () => {
  test('a restarted manager keeps a valid idle lease reusable', async () => {
    const shared = new Map<string, unknown>();
    const h = harness(shared);
    const handle = await h.manager.acquire('7203');
    await h.manager.releaseSuccess(handle, '7203');
    const restarted = createWarmTabLeaseManager(h.deps);

    await restarted.reconcile();
    const reused = await restarted.acquire('7203');

    expect(h.removes).toHaveLength(0);
    expect(reused.mode).toBe('warm_owned_same_code');
  });

  test('a restarted manager closes an expired idle lease', async () => {
    const h = harness();
    const handle = await h.manager.acquire('7203');
    await h.manager.releaseSuccess(handle, '7203');
    h.setNow(NOW + SHIKIHO_WARM_TAB_IDLE_MS);
    const restarted = createWarmTabLeaseManager(h.deps);

    await restarted.reconcile();

    expect(h.removes).toEqual([100]);
    expect(storedLease(h.session)).toBeUndefined();
  });

  test('a restarted manager closes a stale capturing lease', async () => {
    const h = harness();
    await h.manager.acquire('7203');
    const restarted = createWarmTabLeaseManager(h.deps);

    await restarted.reconcile();

    expect(h.removes).toEqual([100]);
    expect(storedLease(h.session)).toBeUndefined();
  });

  test('the original manager preserves its active in-memory capture during reconcile', async () => {
    const h = harness();
    await h.manager.acquire('7203');

    await h.manager.reconcile();

    expect(h.removes).toHaveLength(0);
    expect(storedLease(h.session)).toMatchObject({ phase: 'capturing' });
  });

  test('a missing tab removes metadata without attempting another close', async () => {
    const h = harness();
    const handle = await h.manager.acquire('7203');
    await h.manager.releaseSuccess(handle, '7203');
    h.tabs.delete(100);

    await createWarmTabLeaseManager(h.deps).reconcile();

    expect(h.gets).toContain(100);
    expect(h.removes).toHaveLength(0);
    expect(storedLease(h.session)).toBeUndefined();
  });

  test('tabs.get failure removes metadata without a second close attempt', async () => {
    const h = harness();
    const handle = await h.manager.acquire('7203');
    await h.manager.releaseSuccess(handle, '7203');
    h.getFailures.add(100);

    await createWarmTabLeaseManager(h.deps).reconcile();

    expect(h.removes).toHaveLength(0);
    expect(storedLease(h.session)).toBeUndefined();
  });

  test('a delayed alarm enforces maximum age on the next reconciliation event', async () => {
    const h = harness();
    const handle = await h.manager.acquire('7203');
    await h.manager.releaseSuccess(handle, '7203');
    h.setNow(NOW + SHIKIHO_WARM_TAB_MAX_AGE_MS + 1);

    await createWarmTabLeaseManager(h.deps).reconcile();

    expect(h.removes).toEqual([100]);
    expect(storedLease(h.session)).toBeUndefined();
  });
});
