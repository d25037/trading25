import { normalizeShikihoCode } from './contract';
import type { ShikihoTabRequest } from './shikiho-tab-bridge';
import type { TabMessageReply } from './tab-acquisition';
import type { WarmTabLeaseManager } from './warm-tab-lease';

interface ListenerEvent<TListener extends (...args: never[]) => void> {
  addListener(listener: TListener): void;
  removeListener(listener: TListener): void;
}

type AlarmListener = (alarm: { name: string }) => void;
type ActivatedListener = (activeInfo: { tabId: number }) => void;
type RemovedListener = (tabId: number) => void;
type UpdatedListener = (tabId: number, changeInfo: { status?: string }) => void;
type StartupListener = () => void;

export interface ShikihoBackgroundRuntimeDeps {
  leaseManager: WarmTabLeaseManager;
  sendTabMessage(tabId: number, message: ShikihoTabRequest): Promise<TabMessageReply>;
  alarmsOnAlarm: ListenerEvent<AlarmListener>;
  tabsOnActivated: ListenerEvent<ActivatedListener>;
  tabsOnRemoved: ListenerEvent<RemovedListener>;
  tabsOnUpdated: ListenerEvent<UpdatedListener>;
  runtimeOnStartup: ListenerEvent<StartupListener>;
}

function hasShikihoCode(reply: TabMessageReply, tabId: number): boolean {
  if (reply.tabId !== tabId || typeof reply.response !== 'object' || reply.response === null) return false;
  const response = reply.response as Record<string, unknown>;
  return (
    response.type === 'shikiho_code' &&
    typeof response.code === 'string' &&
    normalizeShikihoCode(response.code) === response.code
  );
}

export function startShikihoBackgroundRuntime(deps: ShikihoBackgroundRuntimeDeps): () => void {
  function run(operation: Promise<void>): void {
    void operation.catch(() => undefined);
  }

  async function verifyOwnedTab(tabId: number): Promise<void> {
    if ((await deps.leaseManager.getValidOwnedTabId()) !== tabId) return;
    const hosted = await deps
      .sendTabMessage(tabId, { type: 'probe_shikiho_code' })
      .then((reply) => hasShikihoCode(reply, tabId))
      .catch(() => false);
    if (!hosted) await deps.leaseManager.abandonOwnedTab(tabId);
  }

  const alarmListener: AlarmListener = (alarm) => run(deps.leaseManager.onAlarm(alarm.name));
  const activatedListener: ActivatedListener = ({ tabId }) => run(deps.leaseManager.onActivated(tabId));
  const removedListener: RemovedListener = (tabId) => run(deps.leaseManager.onRemoved(tabId));
  const updatedListener: UpdatedListener = (tabId, changeInfo) => {
    if (changeInfo.status === 'complete') run(verifyOwnedTab(tabId));
  };
  const startupListener: StartupListener = () => run(deps.leaseManager.reconcile());

  deps.alarmsOnAlarm.addListener(alarmListener);
  deps.tabsOnActivated.addListener(activatedListener);
  deps.tabsOnRemoved.addListener(removedListener);
  deps.tabsOnUpdated.addListener(updatedListener);
  deps.runtimeOnStartup.addListener(startupListener);
  run(deps.leaseManager.reconcile());

  return () => {
    deps.alarmsOnAlarm.removeListener(alarmListener);
    deps.tabsOnActivated.removeListener(activatedListener);
    deps.tabsOnRemoved.removeListener(removedListener);
    deps.tabsOnUpdated.removeListener(updatedListener);
    deps.runtimeOnStartup.removeListener(startupListener);
  };
}
